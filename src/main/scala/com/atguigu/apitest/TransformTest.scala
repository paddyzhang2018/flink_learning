package com.atguigu.apitest

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val streamFromFile = env.readTextFile("src\\main\\resources\\sensor.txt")


    //1. 简单的转换算子和聚合算子
    val dataStream: DataStream[SensorReading] = streamFromFile.map( data =>{
      val dataArray = data.split(",")
      SensorReading( dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .keyBy(0)

      //reduce 输出当前传感器最新的温度+10°C，而时间戳是上一次数据的时间+1
      .reduce( (x, y) => SensorReading(x.id, x.timestamp + 1, x.temperature + 10))



    //2. 多流转换算子
    val splitStream = dataStream.split(data=> {
      if ( data.temperature > 30) Seq("High") else Seq("Low")
    })

    val high = splitStream.select("High")
    val low = splitStream.select("Low")
    val all = splitStream.select("High", "Low")

//    high.print("High")
//    low.print("Low")
//    all.print("All")


    //合并流
    val warning = high.map( data=> (data.id, data.temperature))
    val connectedStreams = warning.connect(low)
    val coMapDataStream = connectedStreams.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "Healthy")
    )


    //Unin
    val uninStream = high.union(low)



    //函数类
    dataStream.filter( _.id.startsWith("sensor_6")).print()




    env.execute("Transform test")
  }
}

class MyFilter(v:String) extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith(v)
  }
}

