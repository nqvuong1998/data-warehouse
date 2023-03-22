package com.vuongnguyen.warehouse.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date, TimeZone}
import scala.collection.mutable.ListBuffer

trait DateUtilMethod {
  def addDay(strDate: String, format: String, num: Int): String
  def convertStrDateToFormat(strDate: String,
                             fromFormat: String,
                             toFormat: String): String
  def convertToCalendar(strDate: String, format: String): Calendar
  def getDateString(time: Calendar, format: String): String

  def convertDate(dateStr: String, format: String): Date
  def convertStr(date: Date, format: String): String
  def replaceDateToString(str: String, date: String, formatDate: String): String

  def getCurrentTimeZone: String
  def getListDate(from: String, to: String, formatDate: String): List[String]
  def getStrFirstDayOfMonth(strDate: String, format: String): String
}
object DateUtil extends DateUtilMethod {
  val DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
  val DEFAULT_DATE_FORMAT_2 = "yyyy-MM-dd HH:mm:ss.SSS"
  val UTC_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

  override def addDay(strDate: String, format: String, num: Int): String = {
    val date = DateUtil.convertDate(strDate, format)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, num)
    DateUtil.convertStr(cal.getTime(), format)
  }

  /**
   * convert string of date from "fromFormat" to "toFormat"
   * return string
   */
  override def convertStrDateToFormat(strDate: String,
                                      fromFormat: String,
                                      toFormat: String): String = {
    val date = convertToCalendar(strDate, fromFormat)
    getDateString(date, toFormat)
  }

  override def convertToCalendar(strDate: String, format: String): Calendar = {
    val formatDate = new java.text.SimpleDateFormat(format)
    val date = formatDate.parse(strDate)
    val c = Calendar.getInstance()
    c.setTime(date)
    c
  }

  override def getDateString(time: Calendar, format: String): String = {
    val formatDate = new java.text.SimpleDateFormat(format)
    formatDate.format(time.getTime)
  }

  override def convertDate(dateStr: String, format: String): Date = {
    val dateFormat = new java.text.SimpleDateFormat(format)
    dateFormat.parse(dateStr)
  }

  /**
   * Convert Date to String follow format
   */
  override def convertStr(date: Date, format: String): String = {
    var dateFormat = new java.text.SimpleDateFormat(format)
    return dateFormat.format(date)
  }

  /**
   * replace string date which has specific formatDate into format (which is in between "{{" and "}}")
   * str : string is replaced
   * formatStr: format is replaced in str
   * date: date time is replaced into str
   * formatDate: format date time is replaced into str
   * return string
   */
  override def replaceDateToString(str: String,
                                   date: String,
                                   formatDate: String): String = {
    var pattern = ("\\{{2}(.*?)\\}{2}").r
    var rs = str
    var i = 0;
    for (m <- pattern.findAllIn(str)) {

      var tmp = m.replaceAll("\\{", "").replaceAll("\\}", "")
      var strReplacedDate = convertStrDateToFormat(date, formatDate, tmp)
      //      println(strReplacedDate)
      rs = rs.replaceAll("\\{\\{" + tmp + "\\}\\}", strReplacedDate)
    }
    rs = rs.replaceAll("\\{", "").replaceAll("\\}", "")
    return rs
  }

  override def getCurrentTimeZone: String = {
    val tz = Calendar.getInstance.getTimeZone
    tz.getID
  }

  override def getListDate(from: String,
                           to: String,
                           formatDate: String): List[String] = {
    var result = new ListBuffer[String]()
    var ymdRun = from
    while (!ymdRun.equals(addDay(to, formatDate, 1))) {
      result += ymdRun
      ymdRun = addDay(ymdRun, formatDate, 1)
    }
    result.toList
  }

  override def getStrFirstDayOfMonth(strDate: String, format: String): String = {

    var dateFormat = new SimpleDateFormat(format)
    var date = dateFormat.parse(strDate)
    var cal = Calendar.getInstance()
    cal.setTime(date)
    cal.set(Calendar.DATE, 1)
    return dateFormat.format(cal.getTime())
  }
}
