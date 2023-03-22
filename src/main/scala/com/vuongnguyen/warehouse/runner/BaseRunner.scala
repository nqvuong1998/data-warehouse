package com.vuongnguyen.warehouse.runner

import com.vuongnguyen.warehouse.configuration.{ParamConfig, ProjectConfig}
import com.vuongnguyen.warehouse.processor.BaseProcessor

object BaseRunner {
  def constructProcessor(params: ParamConfig): BaseProcessor = {
    Class
      .forName(params.processor())
      .getConstructor(params.getClass)
      .newInstance(params)
      .asInstanceOf[BaseProcessor]
  }

  def main(args: Array[String]): Unit = {
    ProjectConfig.loadConfig()
    val params = new ParamConfig(args.toSeq)
    val processor = constructProcessor(params)
    processor.run()
    processor.stop()

  }
}
