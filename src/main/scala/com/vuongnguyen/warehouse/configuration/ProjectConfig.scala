package com.vuongnguyen.warehouse.configuration

import org.apache.log4j.Logger
import org.yaml.snakeyaml.Yaml

import java.io.FileInputStream
import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

class ProjectConfig(rootConf: util.LinkedHashMap[String, Any])
    extends Serializable {
  def get(configName: String): Any = {
    rootConf.get(configName)
  }

  def contains(configName: String): Boolean = {
    rootConf.containsKey(configName)
  }

  def getString(configName: String): String = {
    rootConf.get(configName).toString
  }

  def getListString(configName: String): List[String] = {
    rootConf.get(configName) match {
      case list: util.List[_] => list.asScala.map(_.toString).toList
      case any: Any           => List(any.toString)
      case null               => List.empty[String]
    }
  }

  def getStringOrDefault(configName: String, default: String): String = {
    if (contains(configName)) getString(configName)
    else default
  }

  def getConf(configName: String): ProjectConfig = {
    new ProjectConfig(
      rootConf.get(configName).asInstanceOf[util.LinkedHashMap[String, Any]]
    )
  }

  def getMapConf(configName: String): Map[String, ProjectConfig] = {
    toMapConf(rootConf.get(configName))
  }

  def getMap(configName: String): Map[String, String] = {
    toMap(rootConf.get(configName))
  }

  def getListConf(configName: String): List[ProjectConfig] = {
    rootConf.get(configName) match {
      case map: util.LinkedHashMap[String, Any] => List(new ProjectConfig(map))
      case list: util.List[util.LinkedHashMap[String, Any]] =>
        list.asScala
          .map(m => new ProjectConfig(m))
          .toList
      case _ => List.empty[ProjectConfig]
    }
  }

  def asMap: Map[String, String] = {
    toMap(rootConf)
  }

  def asMapConf: Map[String, ProjectConfig] = {
    toMapConf(rootConf)
  }

  def isEmpty: Boolean = {
    rootConf == null || rootConf.isEmpty
  }

  private def toMap(conf: Any): Map[String, String] = {
    if (conf != null) {
      conf
        .asInstanceOf[util.LinkedHashMap[String, Any]]
        .asScala
        .map(t => (t._1, t._2.toString))
        .toMap
    } else Map.empty[String, String]
  }

  private def toMapConf(conf: Any): Map[String, ProjectConfig] = {
    if (conf != null) {
      conf
        .asInstanceOf[util.LinkedHashMap[String, util.LinkedHashMap[String,
                                                                    Any]]]
        .asScala
        .map { case (key, map) => (key, new ProjectConfig(map)) }
        .toMap
    } else Map.empty[String, ProjectConfig]
  }
}

object ProjectConfig {
  private val logger = Logger.getLogger(this.getClass)

  object DEPLOY_ENV {
    val LOCALHOST = "localhost"
    val SANDBOX = "sandbox"
    val PRODUCTION = "production"
  }

  object CONFIG_KEY {
    val SPARK = "spark"
    val PROXY = "proxy"
    val DIM = "dim"
    val FACT = "fact"
    val JAVA = "java"
    val MART = "mart"
  }

  val TEMPLATE_CONFIG_FILE = "src/main/resources/template_config.yml"

  var env = "localhost"
  var mainConfig: ProjectConfig = _
  var sparkConfig: ProjectConfig = _
  var proxyConfig: ProjectConfig = _
  var dimConfig: ProjectConfig = _
  var factConfig: ProjectConfig = _
  var javaConf: ProjectConfig = _
  var martConf: ProjectConfig = _

  def loadConfig(): Unit = {

    env = System.getProperty("appenv", DEPLOY_ENV.SANDBOX)
    logger.info(s"Job is running on environment: $env")

    val filename = System.getProperty("config", TEMPLATE_CONFIG_FILE)
    logger.info(s"Loading config at: $filename")

    mainConfig = new ProjectConfig(new Yaml().load(new FileInputStream(filename)))
    sparkConfig = mainConfig.getConf(CONFIG_KEY.SPARK)
    proxyConfig = mainConfig.getConf(CONFIG_KEY.PROXY)
    dimConfig = mainConfig.getConf(CONFIG_KEY.DIM)
    factConfig = mainConfig.getConf(CONFIG_KEY.FACT)
    javaConf = mainConfig.getConf(CONFIG_KEY.JAVA)
    martConf = mainConfig.getConf(CONFIG_KEY.MART)

  }
}
