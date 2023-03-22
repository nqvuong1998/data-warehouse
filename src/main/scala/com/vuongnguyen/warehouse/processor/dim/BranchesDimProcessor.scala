package com.vuongnguyen.warehouse.processor.dim

import com.vuongnguyen.warehouse.configuration.{ParamConfig, YAMLConf}
import com.vuongnguyen.warehouse.processor.BaseProcessor
import com.vuongnguyen.warehouse.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}

class BranchesDimProcessor (params: ParamConfig, dateUtil: DateUtilMethod = DateUtil,
                            sparkUtil: SparkUtil.type = SparkUtil) extends BaseProcessor(params) {
  def this(params: ParamConfig) {
    this(params, DateUtil, SparkUtil)
  }

  val maxValueOfTimestamp = "9999-12-31 23:59:59.999"

  object CONFIG_KEY {
    val SOURCE = "source"
    val DESTINATION = "destination"
  }

  object SOURCE_CONFIG_KEY {
    val BRANCHES_LOG = "branches_log"
    val BRANCHES_DIM = "branches_dim"
    val ADDRESSES_DIM = "addresses_dim"
    val BRANCH_TYPES_DIM = "branch_types_dim"
  }

  val yamlPath: String = System.getProperty("config")
  val yamlConf: YAMLConf = YAMLConf.fromPath(yamlPath)

  val srcConf: YAMLConf = yamlConf("dim", CONFIG_KEY.SOURCE)
  val destConf: YAMLConf = yamlConf("dim", CONFIG_KEY.DESTINATION)

  override def run(): Unit = {
    logger.info(s"Running spark job '${params.jobName}' with given params: ${params.toString}")
    val branchesLogDf = readSource(srcConf(SOURCE_CONFIG_KEY.BRANCHES_LOG))
    val branchesDimDf = readSource(srcConf(SOURCE_CONFIG_KEY.BRANCHES_DIM))
    val addressesDimDf = readSource(srcConf(SOURCE_CONFIG_KEY.ADDRESSES_DIM))
    val branchTypesDimDf = readSource(srcConf(SOURCE_CONFIG_KEY.BRANCH_TYPES_DIM))
    val mergeBranchesDimDf = processData(branchesLogDf, branchesDimDf, addressesDimDf, branchTypesDimDf)
    writeToDest(mergeBranchesDimDf, destConf)
  }

  /**
   * Read source using config from params. Will read all date into a single dataframe
   *
   * @return a single dataframe of all date
   */
  def readSource(config: YAMLConf): DataFrame = {
    val ymdFrom = params.ymdFrom()
    val ymdTo = params.ymdTo()
    val formatDate = params.formatDate()
    logger.info(s"Reading logs from date '$ymdFrom' to date '$ymdTo' with format '$formatDate'")

    sparkUtil.readDataFrame(config, Some(ymdFrom), Some(ymdTo), Some(formatDate))
  }

  /**
   * Process data from source log to have information for Dim table
   *
   */
  def processData(branchesLogDf: DataFrame, branchesDimDf: DataFrame, addressesDimDf: DataFrame, branchTypesDimDf: DataFrame): DataFrame = {
    logger.info(s"Processing input data frame........")

    val newBranchesDimDf = branchesLogDf.dropDuplicates("branch_id", "updated_time")
      .join(addressesDimDf, Seq("address_id"), "left_outer")
      .drop("address_id")
      .join(branchTypesDimDf, Seq("branch_type_code"), "left_outer")
      .drop("branch_type_code")
      .na.fill(-1, Seq("address_skey", "branch_type_skey"))
      .withColumn("branch_skey", hash(col("branch_id"), col("updated_time")).cast(LongType))
      .withColumn("sys_valid_from", col("updated_time"))
      .withColumn("sys_valid_to", lag(col("sys_valid_from"), 1).over(Window.partitionBy("branch_id").orderBy(col("updated_time").desc)))
      .withColumn("sys_is_current", when(col("sys_valid_to").isNull, lit(true)).otherwise(lit(false)))
      .withColumn("sys_valid_to", when(col("sys_valid_to").isNull, lit(maxValueOfTimestamp).cast(TimestampType)).otherwise(col("sys_valid_to")))
      .withColumn("sys_partition", date_format(col("updated_time"), "yyyyMMdd"))
      .cache()

    val updateBranchesDimJoinCond = branchesDimDf("branch_id") <=> newBranchesDimDf("branch_id") && branchesDimDf("sys_is_current") === true
    val updateBranchesDimDf = newBranchesDimDf
      .withColumn("rn", row_number().over(Window.partitionBy("branch_id").orderBy(col("updated_time").asc)))
      .filter(col("rn") === 1).drop("rn")
      .join(branchesDimDf, updateBranchesDimJoinCond)
      .select(branchesDimDf("branch_skey"), branchesDimDf("branch_id"), branchesDimDf("address_skey"), branchesDimDf("branch_type_skey"), branchesDimDf("updated_time"), branchesDimDf("sys_valid_from") ,newBranchesDimDf("sys_valid_from").as("sys_valid_to"), lit(false).as("sys_is_current"), branchesDimDf("sys_partition"))

    updateBranchesDimDf.unionByName(newBranchesDimDf, allowMissingColumns = true)
      .withColumn("sys_created_time", current_timestamp())
  }

  /**
   * write df to destination
   *
   * @param df df to write
   */
  def writeToDest(df: DataFrame, config: YAMLConf): Unit = {
    logger.info(s"Writing to destination. Previewing schema...")
    df.printSchema

    SparkUtil.writeDataFrame(df, config)
  }
}
