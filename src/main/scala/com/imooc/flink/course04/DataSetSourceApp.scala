package com.imooc.flink.course04

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object DataSetSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //fromCollection(env)
    //textFile(env)
    //csvFile(env)
    //readRecursiveFiles(env)
    readCompressFiles(env)
  }

  //压缩文件方式创建
  def readCompressFiles(env: ExecutionEnvironment): Unit={
    val compressFile = "C:\\Users\\zhang\\Desktop\\工作簿1.csv.gz"
    env.readTextFile(compressFile).print()
  }

  //目录递归查找文件方式创建
  def readRecursiveFiles(env: ExecutionEnvironment):Unit={
    val parameters = new Configuration()
    parameters.setBoolean("recursive.file.enumeration", true)
    env.readTextFile("src\\main\\resources").withParameters(parameters).print()
  }

  //csv文件的方式创建
  def csvFile(env: ExecutionEnvironment): Unit={
    val csvPath ="C:\\Users\\zhang\\Desktop\\工作簿1.csv"
    //env.readCsvFile[(String, Int, String)](csvPath, ignoreFirstLine = true).print()
    //env.readCsvFile[(String, Int)](csvPath, ignoreFirstLine = true).print()

    case class MyCaseClass(name:String, age:Int)
    env.readCsvFile[MyCaseClass](csvPath, ignoreFirstLine = true, includedFields = Array(0,1)).print()
  }

  //从文件的方式创建
  def textFile(env: ExecutionEnvironment):Unit={
    val filePath = "src\\main\\resources\\"
    env.readTextFile(filePath).print()
  }


  //从集合的方式创建
  def fromCollection(env: ExecutionEnvironment): Unit={
    val data = 1 to 10
    env.fromCollection(data).print()
  }
}