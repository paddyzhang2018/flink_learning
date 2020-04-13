package com.atguigu.apitest

import java.util.Properties

import akka.util.LineNumbers.SourceFile
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random



object SourceUser {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //自定义source
    val stream4 = env.addSource(new SensorSource())
    stream4.print("Stream3").setParallelism(1)

    //stream1.print("Stream1").setParallelism(2)
    env.execute("DIY Source Test")
  }

}

class SensorSource() extends SourceFunction[SensorReading]{

  //定义一个flag表示数据源是否正常运行
  var running: Boolean = true

  //取消数据的生成
  override def cancel(): Unit = {

  }

  //正常生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机数生成器
    val rand = new Random()

    //初始化定义一组传感器温度数据
    var curTemp = 1.to(10).map(
      i=>("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )
    //用死循环，产生数据流
    while (running){
      //更新在前一次温度的基础上更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )

      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t => sourceContext.collect(SensorReading( t._1, curTime, t._2))
      )
      //设置时间间隔
      Thread.sleep(500)
  }


  }




}
