package com.vuongnguyen.warehouse.processor.mart

import com.vuongnguyen.warehouse.configuration.{ParamConfig, YAMLConf}
import com.vuongnguyen.warehouse.processor.BaseProcessor
import com.vuongnguyen.warehouse.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

class MonthlyAccountAverageBalanceMartProcessor(params: ParamConfig, dateUtil: DateUtilMethod = DateUtil,
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
    val DAILY_ACCOUNT_CLOSING_BALANCE_MART = "daily_account_closing_balance_mart"
  }

  val yamlPath: String = System.getProperty("config")
  val yamlConf: YAMLConf = YAMLConf.fromPath(yamlPath)

  val srcConf: YAMLConf = yamlConf("mart", CONFIG_KEY.SOURCE)
  val destConf: YAMLConf = yamlConf("mart", CONFIG_KEY.DESTINATION)

  override def run(): Unit = {
    logger.info(s"Running spark job '${params.jobName}' with given params: ${params.toString}")
    val ymdFrom = params.ymdFrom()
    val ymdTo = params.ymdTo()
    val formatDate = params.formatDate()

    var ymdRun = ymdFrom
    while (ymdRun <= ymdTo){

      val closingBalanceMartDf = readSource(srcConf(SOURCE_CONFIG_KEY.DAILY_ACCOUNT_CLOSING_BALANCE_MART), ymdRun, formatDate)

      val averageBalanceMartDf = processData(ymdRun, formatDate, closingBalanceMartDf)

      writeToDest(averageBalanceMartDf, destConf)

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
  def processData(ymd: String, formatDate: String, closingBalanceMartDf: DataFrame): DataFrame = {
    logger.info(s"Processing input data frame........")

    val firstDateOfMonth = DateUtil.convertStrDateToFormat(DateUtil.getStrFirstDayOfMonth(ymd, formatDate), formatDate, "yyyyMMdd")
    val currentDateOfMonth = DateUtil.convertStrDateToFormat(ymd, formatDate, "yyyyMMdd")

    val averageBalanceMartDf = closingBalanceMartDf
      .filter(col("sys_partition") >= firstDateOfMonth && col("sys_partition") <= currentDateOfMonth)
      .withColumn("average_balance", avg(col("closing_balance")).over(Window.partitionBy( "account_number")))
      .withColumn("rn", row_number().over(Window.partitionBy("account_number").orderBy(col("updated_time").desc)))
      .filter(col("rn") === 1)
      .select("account_skey", "account_number", "average_balance", "updated_time", "date_str")
      .withColumn("month_str", lit(DateUtil.convertStrDateToFormat(ymd, formatDate, "yyyy-MM")))
      .withColumn("account_average_balance_skey", hash(col("account_number"), col("updated_time"), col("date_str"), col("month_str")).cast(LongType))
      .withColumn("sys_partition", lit(currentDateOfMonth))
      .withColumn("sys_created_time", current_timestamp())
      .drop("date_str")

    averageBalanceMartDf
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