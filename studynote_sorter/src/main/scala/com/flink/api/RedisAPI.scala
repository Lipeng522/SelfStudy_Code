package com.flink.api

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.kafka.clients.consumer.ConsumerConfig

object RedisAPI {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"sensor_group")

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), props))
    val sensorStream: DataStream[SensorReading] = inputStream.map(line => {
      val fields: Array[String] = line.split(",")
      SensorReading(fields(0), fields(1).toLong, fields(2).toDouble)
    })

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop102")
      .setPort(6379)
      .build()

    sensorStream.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper()))
    env.execute()
  }

  class MyRedisMapper() extends RedisMapper[SensorReading]{
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"sensor")
    override def getKeyFromData(t: SensorReading): String = t.id
    override def getValueFromData(t: SensorReading): String = t.temperature.toString
  }
}
