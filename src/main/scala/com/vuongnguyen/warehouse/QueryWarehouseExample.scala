package com.vuongnguyen.warehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object QueryWarehouseExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("QueryWarehouseExample")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder
      .config(conf = conf)
      .appName("QueryWarehouseExample")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    println("-------------------------Start------------------------")

    //QUERY 1
    val transactions_fact = spark.read.format("org.apache.hudi").load("/Users/lap14151/Downloads/Code/data-warehouse/data/warehouse-test/transactions_fact/*")
    transactions_fact.createOrReplaceTempView("transactions_fact")

    val transaction_types_dim = spark.read.format("org.apache.hudi").load("/Users/lap14151/Downloads/Code/data-warehouse/data/warehouse-test/transaction_types_dim/*")
    transaction_types_dim.createOrReplaceTempView("transaction_types_dim")

    val accounts_dim = spark.read.format("org.apache.hudi").load("/Users/lap14151/Downloads/Code/data-warehouse/data/warehouse-test/accounts_dim/*")
    accounts_dim.createOrReplaceTempView("accounts_dim")

    spark.sql(
      """SELECT b.transaction_type_code, c.account_number, sum(a.transaction_amount) FROM transactions_fact a
        |INNER JOIN transaction_types_dim b ON a.transaction_type_skey = b.transaction_type_skey
        |INNER JOIN accounts_dim c ON a.account_skey = c.account_skey
        |WHERE a.transaction_timestamp >= '2023-03-20 00:00:00.000'
        |GROUP BY b.transaction_type_code, c.account_number""".stripMargin).show(10, false)

    //QUERY 2
    val customers_dim = spark.read.format("org.apache.hudi").load("/Users/lap14151/Downloads/Code/data-warehouse/data/warehouse-test/customers_dim/*")
    customers_dim.createOrReplaceTempView("customers_dim")

    val branches_dim = spark.read.format("org.apache.hudi").load("/Users/lap14151/Downloads/Code/data-warehouse/data/warehouse-test/branches_dim/*")
    branches_dim.createOrReplaceTempView("branches_dim")

    spark.sql(
      """SELECT a.sys_partition,b.branch_id,COUNT(DISTINCT customer_id) AS count FROM customers_dim a INNER JOIN branches_dim b ON a.branch_skey = b.branch_skey
        |GROUP BY a.sys_partition,b.branch_id ORDER BY a.sys_partition,b.branch_id
        |""".stripMargin).show(10, false)

    // QUERY 3
    val average_balance = spark.read.format("org.apache.hudi").load("/Users/lap14151/Downloads/Code/data-warehouse/data/warehouse-test/monthly_account_average_balance_mart/*")
    average_balance.createOrReplaceTempView("average_balance")

    spark.sql(
      """SELECT month_str,account_number,average_balance FROM average_balance
        |WHERE sys_partition = '20230321'""".stripMargin).show(10, false)

  }
}
