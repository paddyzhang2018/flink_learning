package com.atguigu.wc

import org.apache.flink.api.scala._


//批处理的wordcount
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inputPath = "src/main/resources/hello.txt"
    val inputData = env.readTextFile(inputPath)

    //WordCount处理，切分数据得到word，
    val wordCountDataSet = inputData.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()
  }

}
