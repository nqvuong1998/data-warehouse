package com.vuongnguyen.warehouse.processor.fact

import com.vuongnguyen.warehouse.configuration.{ParamConfig, YAMLConf}
import com.vuongnguyen.warehouse.processor.BaseProcessor
import com.vuongnguyen.warehouse.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

class TransactionsFactProcessor(params: ParamConfig, dateUtil: DateUtilMethod = DateUtil,
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
    val TRANSACTIONS_LOG = "transactions_log"
    val TRANSACTION_TYPES_DIM = "transaction_types_dim"
    val ACCOUNTS_DIM = "accounts_dim"
  }

  val yamlPath: String = System.getProperty("config")
  val yamlConf: YAMLConf = YAMLConf.fromPath(yamlPath)

  val srcConf: YAMLConf = yamlConf("fact", CONFIG_KEY.SOURCE)
  val destConf: YAMLConf = yamlConf("fact", CONFIG_KEY.DESTINATION)

  override def run(): Unit = {
    logger.info(s"Running spark job '${params.jobName}' with given params: ${params.toString}")
    val transactionsLogDf = readSource(srcConf(SOURCE_CONFIG_KEY.TRANSACTIONS_LOG))
    val transactionTypesDimDf = readSource(srcConf(SOURCE_CONFIG_KEY.TRANSACTION_TYPES_DIM))
    val accountsDimDf = readSource(srcConf(SOURCE_CONFIG_KEY.ACCOUNTS_DIM))
    val transactionsFactDf = processData(transactionsLogDf, transactionTypesDimDf, accountsDimDf)
    writeToDest(transactionsFactDf, destConf)
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
  def processData(transactionsLogDf: DataFrame, transactionTypesDimDf: DataFrame, accountsDimDf: DataFrame): DataFrame = {
    logger.info(s"Processing input data frame........")
    val extractSkeyAccountsDimJoinCond = transactionsLogDf("account_number") <=> accountsDimDf("account_number") && transactionsLogDf("transaction_timestamp") >= accountsDimDf("sys_valid_from") && transactionsLogDf("transaction_timestamp") < accountsDimDf("sys_valid_to")

    val transactionsFactDf = transactionsLogDf.dropDuplicates("transaction_id", "transaction_timestamp")
      .join(transactionTypesDimDf, Seq("transaction_type_code"), "left_outer")
      .drop("transaction_type_code")
      .join(accountsDimDf, extractSkeyAccountsDimJoinCond, "left_outer")
      .drop("account_number", "sys_valid_from", "sys_valid_to")
      .na.fill(-1, Seq("transaction_type_skey", "account_skey"))
      .withColumn("transaction_skey", hash(col("transaction_id"), col("transaction_timestamp")).cast(LongType))
      .withColumn("sys_partition", date_format(col("transaction_timestamp"), "yyyyMMdd"))
      .withColumn("sys_created_time", current_timestamp())

    transactionsFactDf
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