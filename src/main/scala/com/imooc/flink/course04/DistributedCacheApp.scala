package com.imooc.flink.course04

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConverters._


object DistributedCacheApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val filepath = "src\\main\\resources\\test\\hello.txt"

    //1. 注册一个本地文件
    env.registerCachedFile(filepath, "zhang-dc")
    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

    data.map(new RichMapFunction[String, String] {

      //2. 在open方法中获取到分布式缓存的内容
      override def open(parameters: Configuration): Unit = {
        val dcFile = getRuntimeContext.getDistributedCache().getFile("zhang-dc")
        val lines = FileUtils.readLines(dcFile)//java

        /*
        此时会出现一个异常，java集合和scala集合不兼容的问题
         */
        for (ele <- lines.asScala){
          println(ele)
        }

      }

      override def map(value: String): String = {
        value
      }
    }).print()


  }
}
