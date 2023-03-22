package com.vuongnguyen.warehouse.configuration

import org.rogach.scallop._

class ParamConfig(arguments: Seq[String]) extends ScallopConf(arguments) {
  val ymdFrom: ScallopOption[String] = opt[String](name = "ymd-from", required = false)
  val ymdTo: ScallopOption[String] = opt[String](name = "ymd-to", required = false)
  val formatDate: ScallopOption[String] = opt[String](name = "format-date", required = false)
  val jobName: ScallopOption[String] = opt[String](name = "job-name", required = false)
  val processor: ScallopOption[String] = opt[String](name = "processor", required = false)
  verify()
}
