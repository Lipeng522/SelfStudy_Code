package com.flink.state

import java.util.Properties

import com.flink.api.SensorReading
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

object StateDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"sensor_group")

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), props))
    val sensorStream: DataStream[SensorReading] = inputStream
      .map(line => {
        val fields: Array[String] = line.split(",")
        SensorReading(fields(0), fields(1).toLong, fields(2).toDouble)})


    val warningStream = sensorStream.keyBy( _.id )
    //      .flatMap( new TempChangeWarning(10.0) )  -->自定义RichFlatMap函数，生命周期内声明状态
      .flatMapWithState[(String, Double, Double), Double]( //直接调用带有状态的API
      {
        case (inputData: SensorReading, None) => (List.empty, Some(inputData.temperature))
        case (inputData: SensorReading, lastTemp: Some[Double]) =>
          val diff = (inputData.temperature - lastTemp.get).abs
          if( diff > 10.0 ){
            ( List((inputData.id, lastTemp.get, inputData.temperature)), Some(inputData.temperature) )
          } else {
            ( List.empty, Some(inputData.temperature) )
          }
        }
      )
    warningStream.print()

    env.execute("state test job")
  }
}

// 自定义RichFlatMapFunction，实现温度跳变检测报警功能
class TempChangeWarning(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
  // 定义状态，保存上一个温度值
  private var lastTempState: ValueState[Double] = _
  val defaultTemp: Double = -273.15

  // 加入一个标识位状态，用来表示是否出现过当前传感器数据
  private var isOccurState: ValueState[Boolean] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double], defaultTemp) )
    isOccurState = getRuntimeContext.getState( new ValueStateDescriptor[Boolean]("is-occur", classOf[Boolean]) )
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 从状态中获取上次温度值
    val lastTemp = lastTempState.value()

    // 跟当前温度作比较，如果大于阈值，输出报警信息
    val diff = (value.temperature - lastTemp).abs
    //    if( diff > threshold && lastTemp != defaultTemp ){
    if( isOccurState.value() && diff > threshold ){
      out.collect( (value.id, lastTemp, value.temperature) )
    }

    // 更新状态
    lastTempState.update(value.temperature)
    isOccurState.update(true)
  }
}

class MyStateOperator extends RichMapFunction[SensorReading, String]{

  //  var myState: ValueState[Int] = _
  lazy val myState: ValueState[Int] = getRuntimeContext.getState[Int](new ValueStateDescriptor[Int]("myInt", classOf[Int]))

  override def open(parameters: Configuration): Unit = {
  //    myState = getRuntimeContext.getState[Int](new ValueStateDescriptor[Int]("myInt", classOf[Int]))
  }

  override def map(value: SensorReading): String = {
    myState.value()
    myState.update(10)
    ""
  }
}