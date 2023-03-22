package com.vuongnguyen.warehouse

import com.vuongnguyen.warehouse.runner.BaseRunner
import com.vuongnguyen.warehouse.utils.DateUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RunProcessorExample {
  def main(args: Array[String]): Unit = {

    val mapping = List(
      Seq("branch_types_dim", "dim.SimpleDimProcessor", "dim/branch_types_dim.yaml"),
      Seq("addresses_dim", "dim.SimpleDimProcessor", "dim/addresses_dim.yaml"),
      Seq("account_types_dim", "dim.SimpleDimProcessor", "dim/account_types_dim.yaml"),
      Seq("transaction_types_dim", "dim.SimpleDimProcessor", "dim/transaction_types_dim.yaml"),
      Seq("branches_dim", "dim.BranchesDimProcessor", "dim/branches_dim.yaml"),
      Seq("customers_dim", "dim.CustomersDimProcessor", "dim/customers_dim.yaml"),
      Seq("accounts_dim", "dim.AccountsDimProcessor", "dim/accounts_dim.yaml"),
      Seq("transactions_fact", "fact.TransactionsFactProcessor", "fact/transactions_fact.yaml"),
      Seq("monthly_account_average_balance_mart", "mart.MonthlyAccountAverageBalanceMartProcessor", "mart/monthly_account_average_balance_mart.yaml")
    )


    val ymdFrom = "20230320"
    val ymdTo = "20230321"
    val formatDate = "yyyyMMdd"
    var ymdRun = ymdFrom
    while (ymdRun <= ymdTo) {
      // Remove ymdRun->now partition of paths

      for (value <- mapping) {
        val conf = new SparkConf()
          .setMaster("local[*]")
          .setAppName("RunProcessorExample")
          .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        val spark = SparkSession.builder
          .config(conf = conf)
          .appName("RunProcessorExample")
          .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val tableName = value(0)
        val processorName = value(1)
        val configName = value(2)
        val fileName = s"/Users/lap14151/Downloads/Code/data-warehouse/config/production/${configName}"
        System.setProperty("appenv", "production")
        System.setProperty("config", fileName)

        val args = Array(
          "--ymd-from", s"${ymdRun}",
          "--ymd-to", s"${ymdRun}",
          "--format-date", s"${formatDate}",
          "--job-name", s"${tableName} Processor",
          "--processor", s"com.vuongnguyen.warehouse.processor.${processorName}")
        BaseRunner.main(args)

        spark.stop()
      }

      ymdRun = DateUtil.addDay(ymdRun, formatDate, 1) //daily
    }
  }
}