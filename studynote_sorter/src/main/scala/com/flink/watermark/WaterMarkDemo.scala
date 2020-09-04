package com.flink.watermark

import java.util.Properties
import com.flink.api.SensorReading
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

object WaterMarkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100)

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"sensor_group")

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), props))

    val sensorStream: DataStream[SensorReading] = inputStream
      .map(line => {
      val fields: Array[String] = line.split(",")
      SensorReading(fields(0), fields(1).toLong, fields(2).toDouble)})
      //延迟时间和水位线放在source源附近
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
      override def extractTimestamp(element: SensorReading) = element.timeStamp * 1000 //毫秒时间戳
    })

    //开窗
    sensorStream.keyBy(0)
      .timeWindow(Time.seconds(10)) //开窗
      .allowedLateness(Time.minutes(1)) //设置允许等待的时间
      .sideOutputLateData(new OutputTag[SensorReading]("side output")) //设置掉队的进入侧输出流
      .reduce(new MyMaxTemp()) //窗口聚合函数
    env.execute("waterMark Demo")
  }
}
// 自定义取窗口最大温度值的聚合函数
case class MyMaxTemp() extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    SensorReading(value1.id, value2.timeStamp + 1, value1.temperature.max(value2.temperature))
}

//自定义一个求平均温度的聚合函数
class MyAvgTemp() extends AggregateFunction[SensorReading, (String, Double, Int), (String, Double)]{
  override def add(value: SensorReading, accumulator: (String, Double, Int)): (String, Double, Int) =
    ( value.id, accumulator._2 + value.temperature, accumulator._3 + 1 )
  override def createAccumulator(): (String, Double, Int) = ("", 0.0, 0)
  override def getResult(accumulator: (String, Double, Int)): (String, Double) =
    ( accumulator._1, accumulator._2 / accumulator._3 )
  override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) =
    ( a._1, a._2 + b._2, a._3 + b._3 )
}