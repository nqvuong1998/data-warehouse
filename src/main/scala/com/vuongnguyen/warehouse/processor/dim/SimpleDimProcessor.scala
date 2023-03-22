package com.vuongnguyen.warehouse.processor.dim

import com.vuongnguyen.warehouse.configuration.{ParamConfig, YAMLConf}
import com.vuongnguyen.warehouse.processor.BaseProcessor
import com.vuongnguyen.warehouse.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, hash}
import org.apache.spark.sql.types.LongType

class SimpleDimProcessor(params: ParamConfig, dateUtil: DateUtilMethod = DateUtil,
                         sparkUtil: SparkUtil.type = SparkUtil) extends BaseProcessor(params) {
  def this(params: ParamConfig) {
    this(params, DateUtil, SparkUtil)
  }

  object CONFIG_KEY {
    val SOURCE = "source"
    val DESTINATION = "destination"
  }

  val yamlPath: String = System.getProperty("config")
  val yamlConf: YAMLConf = YAMLConf.fromPath(yamlPath)

  val srcConf: YAMLConf = yamlConf("dim", CONFIG_KEY.SOURCE)
  val destConf: YAMLConf = yamlConf("dim", CONFIG_KEY.DESTINATION)

  override def run(): Unit = {
    logger.info(s"Running spark job '${params.jobName}' with given params: ${params.toString}")
    val sourceDf = readSource(srcConf)
    val destDf = processData(sourceDf)
    destDf.show()
    writeToDest(destDf, destConf)
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
  def processData(df: DataFrame): DataFrame = {
    logger.info(s"Processing input data frame........")
    val surrogateKeyName = yamlConf("dim", "surrogate_key_name").asString
    val businessKeyName = yamlConf("dim", "business_key_name").asString
    val timeFieldName = yamlConf("dim", "time_field_name").asString

    df
      .withColumn(surrogateKeyName, hash(col(businessKeyName)).cast(LongType))
      .withColumn("sys_partition", date_format(col(timeFieldName), "yyyyMMdd"))
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
