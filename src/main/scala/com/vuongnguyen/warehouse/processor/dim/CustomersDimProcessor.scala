package com.vuongnguyen.warehouse.processor.dim

import com.vuongnguyen.warehouse.configuration.{ParamConfig, YAMLConf}
import com.vuongnguyen.warehouse.processor.BaseProcessor
import com.vuongnguyen.warehouse.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}

class CustomersDimProcessor(params: ParamConfig, dateUtil: DateUtilMethod = DateUtil,
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
    val CUSTOMERS_LOG = "customers_log"
    val CUSTOMERS_DIM = "customers_dim"
    val ADDRESSES_DIM = "addresses_dim"
    val BRANCHES_DIM = "branches_dim"
  }

  val yamlPath: String = System.getProperty("config")
  val yamlConf: YAMLConf = YAMLConf.fromPath(yamlPath)

  val srcConf: YAMLConf = yamlConf("dim", CONFIG_KEY.SOURCE)
  val destConf: YAMLConf = yamlConf("dim", CONFIG_KEY.DESTINATION)

  override def run(): Unit = {
    logger.info(s"Running spark job '${params.jobName}' with given params: ${params.toString}")
    val customersLogDf = readSource(srcConf(SOURCE_CONFIG_KEY.CUSTOMERS_LOG))
    val customersDimDf = readSource(srcConf(SOURCE_CONFIG_KEY.CUSTOMERS_DIM))
    val addressesDimDf = readSource(srcConf(SOURCE_CONFIG_KEY.ADDRESSES_DIM))
    val branchesDimDf = readSource(srcConf(SOURCE_CONFIG_KEY.BRANCHES_DIM))
    val mergeCustomersDimDf = processData(customersLogDf, customersDimDf, addressesDimDf, branchesDimDf)
    writeToDest(mergeCustomersDimDf, destConf)
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
  def processData(customersLogDf: DataFrame, customersDimDf: DataFrame, addressesDimDf: DataFrame, branchesDimDf: DataFrame): DataFrame = {
    logger.info(s"Processing input data frame........")
    val extractSkeyBranchesDimJoinCond = customersLogDf("branch_id") <=> branchesDimDf("branch_id") && customersLogDf("updated_time") >= branchesDimDf("sys_valid_from") && customersLogDf("updated_time") < branchesDimDf("sys_valid_to")

    val newCustomersDimDf = customersLogDf.dropDuplicates("customer_id", "updated_time")
      .join(addressesDimDf, Seq("address_id"), "left_outer")
      .drop("address_id")
      .join(branchesDimDf, extractSkeyBranchesDimJoinCond, "left_outer")
      .drop("branch_id", "sys_valid_from", "sys_valid_to")
      .na.fill(-1, Seq("address_skey", "branch_skey"))
      .withColumn("customer_skey", hash(col("customer_id"), col("updated_time")).cast(LongType))
      .withColumn("sys_valid_from", col("updated_time"))
      .withColumn("sys_valid_to", lag(col("sys_valid_from"), 1).over(Window.partitionBy("customer_id").orderBy(col("updated_time").desc)))
      .withColumn("sys_is_current", when(col("sys_valid_to").isNull, lit(true)).otherwise(lit(false)))
      .withColumn("sys_valid_to", when(col("sys_valid_to").isNull, lit(maxValueOfTimestamp).cast(TimestampType)).otherwise(col("sys_valid_to")))
      .withColumn("sys_partition", date_format(col("updated_time"), "yyyyMMdd"))
      .cache()

    val updateCustomersDimJoinCond = customersDimDf("customer_id") <=> newCustomersDimDf("customer_id") && customersDimDf("sys_is_current") === true
    val updateCustomersDimDf = newCustomersDimDf
      .withColumn("rn", row_number().over(Window.partitionBy("customer_id").orderBy(col("updated_time").asc)))
      .filter(col("rn") === 1).drop("rn")
      .join(customersDimDf, updateCustomersDimJoinCond)
      .select(customersDimDf("customer_skey"), customersDimDf("customer_id"), customersDimDf("address_skey"), customersDimDf("branch_skey"), customersDimDf("date_of_birth"), customersDimDf("description"), customersDimDf("updated_time"), customersDimDf("sys_valid_from") ,newCustomersDimDf("sys_valid_from").as("sys_valid_to"), lit(false).as("sys_is_current"), customersDimDf("sys_partition"))

    updateCustomersDimDf.unionByName(newCustomersDimDf, allowMissingColumns = true)
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
