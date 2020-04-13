package com.atguigu.wc


import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//流处理WordCount
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //接受一个socket文本流
    val dataStream = env.socketTextStream("localhost",7778)

    //对每条数据处理
    val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print().setParallelism(2)

    //启动executor
    env.execute("Stream Word Count")
  }
}
