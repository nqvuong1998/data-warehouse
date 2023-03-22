package com.vuongnguyen.warehouse.utils

import com.vuongnguyen.warehouse.configuration.YAMLConf
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkUtil {
  val logger: Logger = Logger.getLogger(this.getClass)

  def generalRead(
                            sparkSession: SparkSession,
                            format: String,
                            paths: Option[Seq[String]] = None,
                            options: Option[Map[String, String]] = None,
                            schema: Option[StructType] = None
                          ): DataFrame = {
    var reader = sparkSession.read
      .format(format)

    if (options.nonEmpty) {
      val mapOption = options.get
      val optionRead = if (mapOption.contains("query") && mapOption.contains("dbtable")) {
        mapOption - "dbtable"
      } else mapOption
      reader = reader.options(optionRead)
    }

    if (schema.nonEmpty) {
      reader = reader.schema(schema.get)
    }

    try {
      if (paths.nonEmpty) {
        reader.load(paths.get: _*)
      } else {
        reader.load()
      }
    } catch {
      case e: Exception =>
        logger.warn("Spark read error, returning empty dataframe", e)
        if (schema.nonEmpty) {
          sparkSession.createDataFrame(
            sparkSession.sparkContext.emptyRDD[Row],
            schema.get
          )
        } else {
          sparkSession.emptyDataFrame
        }
    }
  }

  def generalWrite(
                             df: DataFrame,
                             mode: String,
                             format: String,
                             path: Option[String] = None,
                             options: Option[Map[String, String]] = None,
                             partitionFields: Option[Seq[String]] = None
                           ): Boolean = {
    var writer = df.write
      .mode(mode)
      .format(format)

    if (options.nonEmpty) {
      writer = writer.options(options.get)
    }

    if (partitionFields.nonEmpty) {
      writer = writer.partitionBy(partitionFields.get: _*)
    }

    try {
      if (path.nonEmpty) {
        writer.save(path.get)
      } else {
        writer.save()
      }
      true
    } catch {
      case e: Exception =>
        logger.error(s"Spark write error with exception: ${e.getMessage}", e)
        false
    }
  }

  /**
   * Read source using config. Will read all date into a single dataframe
   *
   * @return a single dataframe of all date
   */
  def readDataFrame(
                     conf: YAMLConf,
                     ymdFrom: Option[String] = None,
                     ymdTo: Option[String] = None,
                     ymdFormat: Option[String] = None,
                     additionalOptions: Option[Map[String, String]] = None
                   )(implicit sparkSession: SparkSession): DataFrame = {
    val url = conf("url").asString
    val option = conf.getAsOrElse[Map[String, String]]("option", Map.empty)
    val paths = if (conf.contains("path")) {
      // create list input paths by date & filter out invalid path
      val inputPath = url + conf("path").asString
      val inputList = DateUtil
        .getListDate(ymdFrom.get, ymdTo.get, ymdFormat.get)
        .map(date => DateUtil.replaceDateToString(inputPath, date, ymdFormat.get))

      // read all paths
      logger.info(
        s"Reading path '$inputPath' from date '${ymdFrom.get}' to date '${ymdTo.get}', format '${ymdFormat.get}'..."
      )
      println(inputList)
      Some(inputList)
    } else {
      None
    }

    // if option has 'dbtable' or 'query', read from database instead
    if (option.contains("dbtable") || option.contains("query")) {
      val tableName = if (option.contains("dbtable")) {
        option("dbtable")
      } else {
        try {
          val query = option("query").split('\n').map(_.trim).mkString(" ")
          val Pattern = """.*from\s+([a-zA-Z0-9\._]+.*)""".r
          val Pattern(tableName) = query.toLowerCase
          tableName
        } catch {
          case e: Exception =>
            logger.info("Error parse table name.", e)
            ""
        }
      }
      logger.info(s"Reading table '$tableName'...")
    }

    // parse schema from config if exists
    val schema = if (conf.contains("schema")) {
      Some(SparkUtil.buildStruct(conf("schema")))
    } else {
      None
    }

    generalRead(
      sparkSession = sparkSession,
      format = conf("format").asString,
      options = Some(option + ("url" -> url) ++ additionalOptions.getOrElse(Map.empty)),
      paths = paths,
      schema = schema
    )
  }

  def writeDataFrame(df: DataFrame, conf: YAMLConf, dfName: Option[String] = None)(implicit
                                                                                   sparkSession: SparkSession
  ): Unit = {
    // if config 'path' is set, write to path at date toDate + 1 instead
    val path = if (conf.contains("path")) {
      val path = conf("url").asString + conf("path").asString
      logger.info(s"Write '${dfName.getOrElse("dataframe")}' into '$path'...")
      Some(path)
    } else {
      None
    }

    // if option has 'dbtable' or 'query', write to database instead
    val option = conf.getAsOrElse[Map[String, String]]("option", Map.empty) - "query"
    if (option.contains("dbtable")) {

      logger.info(s"Write '${dfName.getOrElse("dataframe")}' into table '${option("dbtable")}'...")
    }

    // if option has max_connection, coalesce to it
    val partitionNum = if (option.contains("max_connection")) {
      option("max_connection").toInt
    } else {
      sparkSession.sparkContext.defaultParallelism * 2
    }

    // if option contains partition_field, partition by them
    val partitionFields = if (option.contains("partition_field")) {
      Some(option("partition_field").split(',').map(_.trim).toSeq)
    } else {
      None
    }

    // calling write
    val result = generalWrite(
      df = df.coalesce(partitionNum),
      mode = conf("mode").asString,
      format = conf("format").asString,
      options = Some(option + ("url" -> conf("url").asString)),
      path = path,
      partitionFields = partitionFields
    )
    if (!result)
      throw new Exception("write DataFrame exception")
  }

  def buildStruct(conf: YAMLConf): StructType = {
    StructType(
      conf("field").asMap.map { case (fieldName, fieldType) =>
        StructField(
          fieldName,
          buildDataType(fieldType),
          nullable = true
        )
      }.toSeq
    )
  }

  def buildArray(conf: YAMLConf): ArrayType = {
    ArrayType(
      buildDataType(conf("element_type"))
    )
  }

  def buildMap(conf: YAMLConf): MapType = {
    MapType(
      buildDataType(conf("key_type")),
      buildDataType(conf("value_type"))
    )
  }

  def buildDataType(dataType: YAMLConf): DataType = {
    dataType() match {
      case _: Map[_, _] =>
        dataType("type").asString match {
          case "struct" => buildStruct(dataType)
          case "array" => buildArray(dataType)
          case "map" => buildMap(dataType)
          case other => CatalystSqlParser.parseDataType(other)
        }
      case _: String => CatalystSqlParser.parseDataType(dataType.asString)
    }
  }
}

