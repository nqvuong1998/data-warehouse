package com.vuongnguyen.warehouse.configuration

import org.yaml.snakeyaml.Yaml

import java.io.{FileInputStream, FileNotFoundException}
import java.nio.file.{Files, Paths}
import scala.io.Codec
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class YAMLConf private (conf: Any) extends Serializable {

  def isOfType[T: ClassTag]: Boolean = conf match {
    case _: T => true
    case _ => false
  }

  def isEmpty: Boolean = conf match {
    case null => true
    case iterable: Iterable[_] => iterable.isEmpty
    case _ => false
  }
  def nonEmpty: Boolean = !isEmpty

  def contains(step: Any, steps: Any*): Boolean = nonEmpty && (step match {
    case idx: Int => isOfType[List[_]] && idx >= 0 && idx < asList.size
    case key: String => isOfType[Map[_, _]] && asMap.contains(key)
    case _ => false
  }) && (steps.isEmpty || this(step).contains(steps.head, steps.tail: _*))

  def apply(): Any = conf
  def apply(step: Any, steps: Any*): YAMLConf = get(step, steps: _*).get

  /**
   * Get the underlined value of this YAMLConf
   * @return the underlined value of this YAMLConf
   */
  def get: Any = conf

  /**
   * Access a child element of this config using steps. This operator is equal to
   * {{{Map.get(steps(0)).get(steps(1))...}}}
   * @param step step to reach the child element. Can be eight [[String]] or [[Int]]
   * @param steps steps to reach the child element. Can be eight [[String]] or [[Int]]
   * @return the child element
   */
  def get(step: Any, steps: Any*): Option[YAMLConf] = {
    val next = step match {
      case idx: Int =>
        try Some(asList(idx))
        catch { case _: Throwable => None }
      case key: String =>
        try asMap.get(key)
        catch { case _: Throwable => None }
      case _ => None
    }
    if (next.isEmpty) None
    else if (steps.isEmpty) next
    else next.get.get(steps.head, steps.tail: _*)
  }
  def getOrElse(step: Any, yamlConf: YAMLConf): YAMLConf = get(step).getOrElse(yamlConf)

  /**
   * Same as [[YAMLConf.get]] but with a type provided
   * @param step step to reach the child element. Can be eight [[String]] or [[Int]]
   * @param steps steps to reach the child element. Can be eight [[String]] or [[Int]]
   * @tparam T type of underlined value of the child element
   * @return underlined value of the child element
   */
  def getAs[T: TypeTag](step: Any, steps: Any*): Option[T] = get(step, steps: _*).map(_.as[T])
  def getAsOrElse[T: TypeTag](step: Any, default: T): T = getAs[T](step).getOrElse(default)

  def getOrAs[T: TypeTag](key: String): T =
    if (conf.isInstanceOf[Map[_, _]] && contains(key)) this(key).as[T] else as[T]

  /**
   * Get the string representation of this YAMLConf. Example:
   * {{{
   * one: two
   * two:
   *  - three
   *  - four
   * }}}
   * @return the string representation of this YAMLConf
   */
  def toFormattedString: String = {
    def formatList(list: List[YAMLConf], spaces: String = ""): String = {
      list.foldLeft("") { case (prev, conf) =>
        prev + "\n" + spaces + "- " + formatAsString(conf, spaces + "  ")
      }
    }
    def formatMap(map: Map[String, YAMLConf], spaces: String = ""): String = {
      map.foldLeft("") { case (prev, (key, value)) =>
        prev + "\n" + spaces + key + ": " + formatAsString(value, spaces + "  ")
      }
    }
    def formatAsString(conf: YAMLConf, spaces: String = ""): String = {
      conf() match {
        case _: List[_] => formatList(conf.asList, spaces)
        case _: Map[_, _] => formatMap(conf.asMap, spaces)
        case _ => conf.asString
      }
    }
    formatAsString(this)
  }

  override def toString: String = conf.toString

  /**
   * Get the underlined value of this config as type T
   * @tparam T T type of underlined value
   * @return the underlined value of this config as type T
   */
  def as[T: TypeTag]: T = {
    val rs = typeOf[T] match {
      case t if t =:= typeOf[String] => asString
      case t if t =:= typeOf[Boolean] => asBoolean
      case t if t =:= typeOf[Int] => asInt
      case t if t =:= typeOf[Long] => asLong
      case t if t =:= typeOf[Float] => asFloat
      case t if t =:= typeOf[Double] => asDouble
      case t if t =:= typeOf[Any] => get
      case t if t <:< typeOf[List[String]] => asList.map(_.asString)
      case t if t <:< typeOf[List[Boolean]] => asList.map(_.asBoolean)
      case t if t <:< typeOf[List[Int]] => asList.map(_.asInt)
      case t if t <:< typeOf[List[Long]] => asList.map(_.asLong)
      case t if t <:< typeOf[List[Float]] => asList.map(_.asFloat)
      case t if t <:< typeOf[List[Double]] => asList.map(_.asDouble)
      case t if t <:< typeOf[List[Any]] => asList.map(_.get)
      case t if t <:< typeOf[Map[String, String]] =>
        asMap.map { case (key, value) => key -> value.asString }
      case t if t <:< typeOf[Map[String, Boolean]] =>
        asMap.map { case (key, value) => key -> value.asBoolean }
      case t if t <:< typeOf[Map[String, Int]] =>
        asMap.map { case (key, value) => key -> value.asInt }
      case t if t <:< typeOf[Map[String, Long]] =>
        asMap.map { case (key, value) => key -> value.asLong }
      case t if t <:< typeOf[Map[String, Float]] =>
        asMap.map { case (key, value) => key -> value.asFloat }
      case t if t <:< typeOf[Map[String, Double]] =>
        asMap.map { case (key, value) => key -> value.asDouble }
      case t if t <:< typeOf[Map[String, Any]] =>
        asMap.map { case (key, value) => key -> value.get }
      case other =>
        throw new UnsupportedOperationException(s"Conversion to $other is not supported!")
    }
    rs.asInstanceOf[T]
  }

  def asString: String = toString
  def asBoolean: Boolean = asString.toBoolean
  def asInt: Int = asString.toInt
  def asLong: Long = asString.toLong
  def asFloat: Float = asString.toFloat
  def asDouble: Double = asString.toDouble

  def asList: List[YAMLConf] = conf match {
    case null => List.empty
    case list: List[_] => list.map(elem => new YAMLConf(elem))
    case other => List(new YAMLConf(other))
  }

  def asMap: Map[String, YAMLConf] = conf match {
    case null => Map.empty
    case map: Map[_, _] =>
      map
        .asInstanceOf[Map[String, Any]]
        .map { case (key: String, value: Any) => key -> new YAMLConf(value) }
    case other =>
      throw new UnsupportedOperationException(
        s"Conversion from ${other.getClass} to Map is not supported!"
      )
  }
}

object YAMLConf extends Serializable {

  def empty: YAMLConf = new YAMLConf(null)

  /**
   * Construct a yaml config from Map[String, Any]
   * @param map a map whose value is either Map, List or primitives
   * @return YAMLConf
   */
  def fromMap(map: Map[String, Any]): YAMLConf =
    new YAMLConf(map)

  /**
   * Construct a yaml config from List[Any]
   * @param list a list whose value is either Map, List or primitives
   * @return YAMLConf
   */
  def fromList(list: List[Any]): YAMLConf =
    new YAMLConf(list)

  /**
   * Construct a yaml config from a yaml file path
   * @param yamlPath path to yaml file
   * @return YAMLConf
   */
  def fromPath(yamlPath: String): YAMLConf = {
    if (Files.exists(Paths.get(yamlPath))) {

      val configFileContent = scala.io.Source
        .fromFile(yamlPath)(Codec.UTF8)
        .mkString

      val conf = recursiveToScala(new Yaml().load(new FileInputStream(yamlPath)))
      new YAMLConf(conf)
    } else {
      throw new FileNotFoundException(s"Path does not exist: '$yamlPath'")
    }
  }

  /**
   * Construct a yaml config from a string representation of yaml
   * @param yamlString string representation of yaml
   * @return YAMLConf
   */
  def fromString(yamlString: String): YAMLConf = {
    val conf = recursiveToScala(new Yaml().load(yamlString))
    new YAMLConf(conf)
  }

  private def recursiveToScala(conf: Any): Any = conf match {
    case null => null
    case map: java.util.Map[_, _] =>
      map
        .asInstanceOf[java.util.Map[String, Any]]
        .asScala
        .map { case (key, value) => key -> recursiveToScala(value) }
        .toMap
    case list: java.util.List[_] =>
      list
        .asInstanceOf[java.util.List[String]]
        .asScala
        .map(recursiveToScala)
        .toList
    case primitive @ (_: String | _: Int | _: Long | _: Float | _: Double | _: Boolean) => primitive
    case other =>
      throw new UnsupportedOperationException(
        s"Cannot match type of '$other' in yaml configuration!"
      )
  }

  // scalastyle:off
  object implicits {

    implicit class YAMLConfImplicits(yamlConf: YAMLConf) {

      /**
       * Create a new YAMLConf by append this map to path steps*
       * @param map      map to append
       * @param steps    steps to reach a config
       * @return a new YAMLConf
       */
      def append(map: Map[String, Any], steps: Any*): YAMLConf = {
        if (steps.isEmpty) {
          fromMap(yamlConf.as[Map[String, Any]] ++ map)
        } else {
          val step = steps.head
          val newValue = yamlConf(step).append(map, steps.tail: _*).get
          step match {
            case idx: Int => fromList(yamlConf.as[List[Any]].updated(idx, newValue))
            case key: String => fromMap(yamlConf.as[Map[String, Any]].updated(key, newValue))
            case other =>
              throw new UnsupportedOperationException(
                s"Access using type ${other.getClass} is not supported!"
              )
          }
        }
      }

      /**
       * Create a new YAMLConf by append this list to path steps*
       * @param list     list to append
       * @param steps    steps to reach a config
       * @return a new YAMLConf
       */
      def append(list: List[Any], steps: Any*): YAMLConf = {
        if (steps.isEmpty) {
          fromList(yamlConf.as[List[Any]] ++ list)
        } else {
          val step = steps.head
          val newValue = yamlConf(step).append(list, steps.tail: _*).get
          step match {
            case idx: Int => fromList(yamlConf.as[List[Any]].updated(idx, newValue))
            case key: String => fromMap(yamlConf.as[Map[String, Any]].updated(key, newValue))
            case other =>
              throw new UnsupportedOperationException(
                s"Access using type ${other.getClass} is not supported!"
              )
          }
        }
      }
    }
  }
  // scalastyle:on
}

