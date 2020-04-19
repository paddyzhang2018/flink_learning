package com.imooc.flink.course04

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

object CounterApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")
    val info = data.map(new RichMapFunction[String, String]() {

      //1. 定义计数器

      val counter = new LongCounter()
      override def open(parameters: Configuration): Unit = {
        //2. 注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala", counter)

      }

      override def map(value: String): String = {
        counter.add(1)
        value
      }
    })
    val filepath = "src\\main\\resources\\sink_out\\"
    info.writeAsText(filepath, WriteMode.OVERWRITE).setParallelism(3)
    val jobResult = env.execute("Counter")
    //3. 获取计数器
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")
    println("Nums: " + num)
  }
}
