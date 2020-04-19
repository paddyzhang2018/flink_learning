package com.atguigu.wc


import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

//流处理WordCount
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //接受一个socket文本流
    val dataStream = env.socketTextStream("192.168.8.11",7777)

    //对每条数据处理
    val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .setParallelism(1)

    wordCountDataStream.print()

    //启动executor
    env.execute("Stream Word Count")
  }
}
