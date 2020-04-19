package com.imooc.flink.course04

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object DataSetTransformationApp {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val data = env.fromCollection(List(1,2,3,4,5,6,7,8,9,10))

  def main(args: Array[String]): Unit = {
    //mapFunction(env)
    //filterFunction(env)
    //mapPartitionFunction(env)
    //firstFunction(env)
    //flatMapFunction(env)
    //joinFunction(env)
    outFunction(env)
  }


  //map
  def mapFunction(env: ExecutionEnvironment):Unit={
    //data.map((x:Int) => x + 1).print()
    //data.map((x) => x + 1).print()
    //data.map(x => x + 1).print()
    data.map(_ + 1).print()
  }

  //filter
  def filterFunction(env: ExecutionEnvironment):Unit={
    data.map(_ + 1).filter(_ > 5).print()
  }

  //mappartition[很重要]
  def mapPartitionFunction(env: ExecutionEnvironment):Unit={
    val students = new ListBuffer[String]
    for (i<-1 to 100){
      students.append("student: " + i)
    }

    val data = env.fromCollection(students).setParallelism(2)
    data.mapPartition(x=>{
      val connection = DBUtils.getConnection()
      println(connection + "---")
      //TODO... 保存数据到DB
      DBUtils.returnConnection(connection)
      x
    }).print()
  }


  //first   sort
  def firstFunction(env: ExecutionEnvironment):Unit={
    val info = ListBuffer[(Int, String)]()
    info.append((1, "hadoop"));
    info.append((1, "Spark"));
    info.append((1, "Flink"));
    info.append((2, "Java"));
    info.append((2, "Spring Boot"));
    info.append((3, "Linux"));
    info.append((4, "VUE"));
    val data1 = env.fromCollection(info)
    data1.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print()
  }

  //flatMap
  def flatMapFunction(env:ExecutionEnvironment):Unit= {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")
    val data = env.fromCollection(info)
    data.flatMap(_.split(","))
      .map((_,1)).groupBy(0).sum(1).print()
  }

  //join
  def joinFunction(env: ExecutionEnvironment):Unit={
    val info1 = ListBuffer[(Int, String)]()
    info1.append((1, "zhang"))
    info1.append((2, "zhang1"))
    info1.append((3, "zhang2"))
    info1.append((4, "zhang3"))

    val info2 = ListBuffer[(Int, String)]()
    info2.append((1, "Beijing"))
    info2.append((2, "Shanghai"))
    info2.append((3, "Hangzhou"))
    info2.append((5, "Guangzhou"))


    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.join(data2).where(0).equalTo(0).apply((first, second)=>{
      (first._1, first._2, second._2)
    }).print()
  }

  //out join
  def outFunction(env: ExecutionEnvironment):Unit={
    val info1 = ListBuffer[(Int, String)]()
    info1.append((1, "zhang"))
    info1.append((2, "zhang1"))
    info1.append((3, "zhang2"))
    info1.append((4, "zhang3"))

    val info2 = ListBuffer[(Int, String)]()
    info2.append((1, "Beijing"))
    info2.append((2, "Shanghai"))
    info2.append((3, "Hangzhou"))
    info2.append((5, "Guangzhou"))


    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((first, second)=>{
      if (second == null){
        (first._1, first._2, "_")
      } else {
        (first._1, first._2, second._2)
      }
    }).print()
  }

  //笛卡尔积
  def crossFunction(env:ExecutionEnvironment):Unit={
    val info1 = List("曼联","曼城")
    val info2 = List(3, 1, 0)
  }
}
