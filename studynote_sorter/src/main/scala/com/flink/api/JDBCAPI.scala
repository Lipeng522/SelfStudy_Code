package com.flink.api

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

object JDBCAPI {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"sensor_group")

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), props))
    val tupleStream: DataStream[(String, Long, Double)] = inputStream.map(line => {
      val fields: Array[String] = line.split(",")
      (fields(0), fields(1).toLong, fields(2).toDouble)
    })

    tupleStream.addSink(new MyJDBCSink)

    env.execute("jdbc api")
  }
}

class MyJDBCSink extends RichSinkFunction[(String,Long,Double)]{

  var conn : Connection = _
  var insertStatement: PreparedStatement = _
  var updateStatement: PreparedStatement = _
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","0522")
    insertStatement = conn.prepareStatement("insert into test (id,time,tem) values(?,?,?)")
    updateStatement = conn.prepareStatement("update test set tem = ? where id = ?")
  }

  override def invoke(value: (String, Long, Double), context: SinkFunction.Context[_]): Unit = {
    updateStatement.setDouble(1,value._3)
    updateStatement.setString(2,value._1)
    updateStatement.execute()
    if (updateStatement.getUpdateCount == 0){
      insertStatement.setString(1,value._1)
      insertStatement.setLong(2,value._2)
      insertStatement.setDouble(3,value._3)
      insertStatement.execute()
    }
  }

  override def close(): Unit = {
    insertStatement.close()
    updateStatement.close()
    conn.close()
  }
}