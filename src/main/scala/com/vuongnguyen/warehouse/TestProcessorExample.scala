package com.vuongnguyen.warehouse

import com.vuongnguyen.warehouse.configuration.{ParamConfig, ProjectConfig}
import com.vuongnguyen.warehouse.processor.dim.SimpleDimProcessor
import com.vuongnguyen.warehouse.utils.{DateUtil, SparkUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TestProcessorExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("TestProcessorExample")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder
      .config(conf = conf)
      .appName("TestProcessorExample")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    println("-------------------------Start------------------------")

    val tableName = "addresses_dim"
    val processorName = "dim.SimpleDimProcessor"
    val TEST_CONFIG_FILE: String = s"/Users/lap14151/Downloads/Code/data-warehouse/config/production/dim/${tableName}.yaml"

    System.setProperty("appenv", "production")
    System.setProperty("config", TEST_CONFIG_FILE)
    ProjectConfig.loadConfig()

    val args = Seq(
      "--ymd-from", "20230320",
      "--ymd-to", "20230320",
      "--format-date", "yyyyMMdd",
      "--job-name", s"${tableName} Processor",
      "--processor", s"com.vuongnguyen.warehouse.processor.${processorName}")
    val params = new ParamConfig(args)
    val processor = new SimpleDimProcessor(params, DateUtil, SparkUtil)
    processor.run()

    val result = spark.read.format("org.apache.hudi")
      .load(s"/Users/lap14151/Downloads/Code/data-warehouse/data/warehouse/${tableName}/*")
    result.show(10, false)
  }
}