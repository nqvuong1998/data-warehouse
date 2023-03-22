package com.vuongnguyen.warehouse.processor.dim

import com.vuongnguyen.warehouse.configuration.{ParamConfig, YAMLConf}
import com.vuongnguyen.warehouse.processor.BaseProcessor
import com.vuongnguyen.warehouse.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}

class AccountsDimProcessor(params: ParamConfig, dateUtil: DateUtilMethod = DateUtil,
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
    val ACCOUNTS_LOG = "accounts_log"
    val ACCOUNTS_DIM = "accounts_dim"
    val ACCOUNT_TYPES_DIM = "account_types_dim"
    val CUSTOMERS_DIM = "customers_dim"
    val DAILY_ACCOUNT_CLOSING_BALANCE_MART = "daily_account_closing_balance_mart"
  }

  object DESTINATION_CONFIG_KEY {
    val ACCOUNTS_DIM = "accounts_dim"
    val DAILY_ACCOUNT_CLOSING_BALANCE_MART = "daily_account_closing_balance_mart"
  }

  val yamlPath: String = System.getProperty("config")
  val yamlConf: YAMLConf = YAMLConf.fromPath(yamlPath)

  val srcConf: YAMLConf = yamlConf("dim", CONFIG_KEY.SOURCE)
  val destConf: YAMLConf = yamlConf("dim", CONFIG_KEY.DESTINATION)

  override def run(): Unit = {
    logger.info(s"Running spark job '${params.jobName}' with given params: ${params.toString}")
    val ymdFrom = params.ymdFrom()
    val ymdTo = params.ymdTo()
    val formatDate = params.formatDate()

    var ymdRun = ymdFrom
    while (ymdRun <= ymdTo){
      val accountsLogDf = readSource(srcConf(SOURCE_CONFIG_KEY.ACCOUNTS_LOG), ymdRun, formatDate)
      val accountsDimDf = readSource(srcConf(SOURCE_CONFIG_KEY.ACCOUNTS_DIM), ymdRun, formatDate)
      val accountTypesDimDf = readSource(srcConf(SOURCE_CONFIG_KEY.ACCOUNT_TYPES_DIM), ymdRun, formatDate)
      val customersDimDf = readSource(srcConf(SOURCE_CONFIG_KEY.CUSTOMERS_DIM), ymdRun, formatDate)
      val closingBalanceMartDf = readSource(srcConf(SOURCE_CONFIG_KEY.DAILY_ACCOUNT_CLOSING_BALANCE_MART), DateUtil.addDay(ymdRun, formatDate, -1), formatDate)

      val (mergeAccountsDimDf, newClosingBalanceMartDf) = processData(ymdRun, formatDate, accountsLogDf, accountsDimDf, accountTypesDimDf, customersDimDf, closingBalanceMartDf)

      writeToDest(mergeAccountsDimDf, destConf(DESTINATION_CONFIG_KEY.ACCOUNTS_DIM))
      writeToDest(newClosingBalanceMartDf, destConf(DESTINATION_CONFIG_KEY.DAILY_ACCOUNT_CLOSING_BALANCE_MART))

      ymdRun = DateUtil.addDay(ymdRun, formatDate, 1)
    }
  }

  /**
   * Read source using config from params. Will read all date into a single dataframe
   *
   * @return a single dataframe of all date
   */
  def readSource(config: YAMLConf, ymd: String, formatDate: String): DataFrame = {
    logger.info(s"Reading logs '$ymd' with format '$formatDate'")

    sparkUtil.readDataFrame(config, Some(ymd), Some(ymd), Some(formatDate))
  }

  /**
   * Process data from source log to have information for Dim table
   *
   */
  def processData(ymd: String, formatDate: String, accountsLogDf: DataFrame, accountsDimDf: DataFrame, accountTypesDimDf: DataFrame, customersDimDf: DataFrame, closingBalanceMartDf: DataFrame): (DataFrame, DataFrame) = {
    logger.info(s"Processing input data frame........")
    val extractSkeyCustomersDimJoinCond = accountsLogDf("customer_id") <=> customersDimDf("customer_id") && accountsLogDf("updated_time") >= customersDimDf("sys_valid_from") && accountsLogDf("updated_time") < customersDimDf("sys_valid_to")

    val dedupCols = List[String]("account_number","account_status_code","account_type_skey","customer_skey")
    val newAccountsDimDf = accountsLogDf.dropDuplicates("account_number", "updated_time")
      .join(accountTypesDimDf, Seq("account_type_code"), "left_outer")
      .drop("account_type_code")
      .join(customersDimDf, extractSkeyCustomersDimJoinCond, "left_outer")
      .drop("customer_id", "sys_valid_from", "sys_valid_to")
      .na.fill(-1, Seq("account_type_skey", "customer_skey"))
      .withColumn("rn_dedup", row_number().over(Window.partitionBy(dedupCols.map(col(_)):_*).orderBy(col("updated_time").asc)))
      .withColumn("rn_close", row_number().over(Window.partitionBy("account_number").orderBy(col("updated_time").desc)))
      .filter(col("rn_dedup") === 1 || col("rn_close") === 1)
      .withColumn("account_skey", hash(col("account_number"), col("updated_time")).cast(LongType))
      .cache()

    val newDedupAccountsDimDf = newAccountsDimDf
      .drop("current_balance", "rn_dedup", "rn_close")
      .withColumn("sys_valid_from", col("updated_time"))
      .withColumn("sys_valid_to", lag(col("sys_valid_from"), 1).over(Window.partitionBy("account_number").orderBy(col("updated_time").desc)))
      .withColumn("sys_is_current", when(col("sys_valid_to").isNull, lit(true)).otherwise(lit(false)))
      .withColumn("sys_valid_to", when(col("sys_valid_to").isNull, lit(maxValueOfTimestamp).cast(TimestampType)).otherwise(col("sys_valid_to")))
      .withColumn("sys_partition", date_format(col("updated_time"), "yyyyMMdd"))

    val updateAccountsDimJoinCond = accountsDimDf("account_number") <=> newDedupAccountsDimDf("account_number") && accountsDimDf("sys_is_current") === true
    val updateAccountsDimDf = newDedupAccountsDimDf
      .withColumn("rn", row_number().over(Window.partitionBy("account_number").orderBy(col("updated_time").asc)))
      .filter(col("rn") === 1).drop("rn")
      .join(accountsDimDf, updateAccountsDimJoinCond)
      .select(accountsDimDf("account_skey"), accountsDimDf("account_number"), accountsDimDf("account_type_skey"), accountsDimDf("customer_skey"), accountsDimDf("account_status_code"), accountsDimDf("updated_time"), accountsDimDf("sys_valid_from") ,newDedupAccountsDimDf("sys_valid_from").as("sys_valid_to"), lit(false).as("sys_is_current"), accountsDimDf("sys_partition"))

    val mergeAccountsDimDf = updateAccountsDimDf.unionByName(newDedupAccountsDimDf, allowMissingColumns = true)
      .withColumn("sys_created_time", current_timestamp())

    val newClosingBalanceMartDf = newAccountsDimDf
      .filter(col("rn_close") === 1).drop("rn_close")
      .select("account_skey","account_number","current_balance","updated_time")
      .withColumnRenamed("current_balance", "closing_balance")
      .unionByName(closingBalanceMartDf, allowMissingColumns = true)
      .withColumn("rn", row_number().over(Window.partitionBy("account_number").orderBy(col("updated_time").desc)))
      .filter(col("rn") === 1).drop("rn")
      .withColumn("date_str", lit(DateUtil.convertStrDateToFormat(ymd, formatDate, "yyyy-MM-dd")))
      .withColumn("account_closing_balance_skey", hash(col("account_number"), col("updated_time"), col("date_str")).cast(LongType))
      .withColumn("sys_partition", lit(DateUtil.convertStrDateToFormat(ymd, formatDate, "yyyyMMdd")))
      .withColumn("sys_created_time", current_timestamp())

      (mergeAccountsDimDf, newClosingBalanceMartDf)
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