package com.imooc.flink.course04

import org.apache.flink.api.scala._

object SinkApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = 1.to(10)
    val text = env.fromCollection(data)
    val filePath = "src\\main\\resources\\sink_out1"
    text.writeAsText(filePath)
    env.execute("SinkOut")
  }
}
