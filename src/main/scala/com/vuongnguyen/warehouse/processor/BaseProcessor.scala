package com.vuongnguyen.warehouse.processor

import com.vuongnguyen.warehouse.configuration.ParamConfig
import com.vuongnguyen.warehouse.configuration.ProjectConfig._
import com.vuongnguyen.warehouse.utils.DateUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.TimeZone

abstract class BaseProcessor(params: ParamConfig) {
  val logger: Logger = Logger.getLogger(this.getClass)
  logger.setLevel(Level.INFO)
  for ((k, v) <- javaConf.getMap("java_conf")) {
    System.setProperty(k, v)
  }
  TimeZone.setDefault(null)
  logger.info("timezone: " + DateUtil.getCurrentTimeZone)

  implicit lazy val sparkSession: SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(params.jobName.getOrElse(this.getJobName))
      .config(new SparkConf().setAll(sparkConfig.getMap("spark_conf")))
      .getOrCreate()

    if (sparkConfig.contains("hadoop_conf")) {
      for ((k, v) <- sparkConfig.getMap("hadoop_conf")) {
        spark.sparkContext.hadoopConfiguration.set(k, v)
      }
    }
    spark
  }

  def run(): Unit

  def stop(): Unit = {
    sparkSession.stop()
  }

  def getJobName: String = {
    this.getClass.getSimpleName
  }
}
