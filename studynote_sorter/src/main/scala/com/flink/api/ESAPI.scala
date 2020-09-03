package com.flink.api

import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.http.HttpHost
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object ESAPI {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val props = new Properties()
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
  props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"sensor_group")

  val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), props))
  val sensorStream: DataStream[SensorReading] = inputStream.map(line => {
    val fields: Array[String] = line.split(",")
    SensorReading(fields(0), fields(1).toLong, fields(2).toDouble)
  })

  //DEFINE HTTPHOST
  val httpHosts = new util.ArrayList[HttpHost]
  httpHosts.add(new HttpHost("hadoop102",9200))
  //write to es
  sensorStream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHosts,new MyEsSinkFunction)
    .build()
  )

}

class MyEsSinkFunction extends ElasticsearchSinkFunction[SensorReading ]{
  override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    val map = new util.HashMap[String, String]()
    map.put("id",t.id)
    map.put("temp",t.temperature.toString)
    map.put("ts",t.timeStamp.toString)

    val indexRequest: IndexRequest = Requests.indexRequest()
      .index("sensor")
      .`type`("_doc")
      .source(map)
    requestIndexer.add(indexRequest)
  }
}
case class SensorReading(id: String,timeStamp:Long,temperature:Double)
