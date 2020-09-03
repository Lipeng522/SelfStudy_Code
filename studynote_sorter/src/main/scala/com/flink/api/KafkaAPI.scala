package com.flink.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

object KafkaAPI {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"sensor_group")

    val pprops = new Properties()
    pprops.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    pprops.setProperty(ProducerConfig.ACKS_CONFIG,"1")

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), props))
    val tupleStream: DataStream[(String, Long, Double)] = inputStream.map(line => {
      val fields: Array[String] = line.split(",")
      (fields(0), fields(1).toLong, fields(2).toDouble)
    })

    tupleStream.map(data => data.toString())
      .addSink(new FlinkKafkaProducer011[String]("sensor_out",new SimpleStringSchema(),pprops))
    env.execute("kafka API")
  }
}
