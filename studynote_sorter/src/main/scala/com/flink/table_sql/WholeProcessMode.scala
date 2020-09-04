package com.flink.table_sql

import com.flink.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Kafka, Schema}
import org.apache.flink.types.Row

/**
 * 本code阐述了flink table API & SQL 的整体流程
 */
object WholeProcessMode {
  def main(args: Array[String]): Unit = {
    //1 定义环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2 获取table环境，具体的环境类型，可以参考 TableEnvOldAndNew
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //3 时间语义设置,若设置为时间事件语义，则后边需设置时间措的提取 --------------->>>> event time 相关设置
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //4 获取数据流的方式转换成table
    //*********************************数据流转换成table******************************
      //4.1 Stream流输入，一般是kafka流输入
    val inputStream: DataStream[String] = env.readTextFile("D:\\IdeaWorkSpace\\flink0317\\flink-0317\\src\\main\\resources\\sensor.txt")
    val sensorStream: DataStream[SensorReading] = inputStream.map(line => {
      val fields: Array[String] = line.split(",")
      SensorReading(fields(0), fields(1).toLong, fields(2).toDouble)
    })
      //4.2 提取时间，设置waterMark,若未定义event time，则无需设置   ------------------->>>>event time相关设置
      val eventTimeWaterMarkStream: DataStream[SensorReading] =
        sensorStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading) = element.timeStamp * 1000
      })

    //5 通过数据流在catalog中注册表
    //*********************************常用数据源类型*********************************
    //5.1 通过流数据获取
      //5.1.1 通过流转换获取table,获取process Time语义的table
    val processTimeTable: Table = tableEnv.fromDataStream(eventTimeWaterMarkStream, 'id, 'temperature as 'temp, 'timestamp as 'ts, 'pt.proctime)
      //5.1.2 通过流转换获取table，获取event Time语义的table ----------->>>>>EventTime 相关设置
    val eventTimeTable: Table = tableEnv.fromDataStream(eventTimeWaterMarkStream, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)

    //5.2 通过kafka获取
    //连接kafka
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      //.properties(props) 可以设置成配置文件，直接传入，也可以如下配置
      .property("zookeeper.connect","localhost:2181")
      .property("bootstrap.servers","localhost:9092"))
      .withFormat(new Csv())
      .withSchema(new Schema().field("id",DataTypes.STRING())
        .field("timestemp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE()))
      .createTemporaryTable("kafkainputtable") //创建对应输入的虚拟表，可以直接用于sql方式查询
    //5.3 通过文件获取，如文本文件，execl表等
    val filePath = "D:\\IdeaWorkSpace\\flink0317\\flink-0317\\src\\main\\resources\\sensor.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv()) //Csv 编译器需要引入依赖，适合中间用“，”隔开的类型，中间无空格
      .withSchema(new Schema().field("id",DataTypes.STRING())
        .field("timestemp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE()))
      .createTemporaryTable("fileinputtable")

    //6 表的查询
      //6.1 table API的使用，table API只能由table 类进行调用
      //6.1.1 table API的简单查询
    val selectTable: Table = eventTimeTable.select('id, 'temp)
      .where('id === "sensor_1") //expressions 语法使用时，用半个单引号，=要用 === 表示
    val idTable: Table = selectTable.select('id) //table API可以以上次查询的结果为临时表，进行二次查询
      //6.1.2 table API的聚合操作
    val resultTable3: Table = eventTimeTable.groupBy('id)
      .select('id, 'id.count as 'cnt)  //聚合操作的结果再输出时，需要考虑数据输出的模式

      //6.2 SQL方式的简单查询，sql方式，必须从表或view（非table类）中查询，若是table类，需转化成表或视图
      //6.2.1 从source的映射的虚拟表中进行简单查询
    val fileInputTable: Table = tableEnv.sqlQuery(
      """
        |select id,timestamp,temperature
        |from fileinputtable
        |""".stripMargin)
    tableEnv.createTemporaryView("fileinputtabletrans",fileInputTable) //连续进行sql查询时，中间的table类需转换成view视图
    val tansTable: Table = tableEnv.sqlQuery(
      """
        |select if,timestamp
        |from fileinputtabletrans
        |""".stripMargin)

      //6.2.2 sql的聚合查询
      val resultTable: Table = tableEnv.sqlQuery(
        """
          |select id,count(id) as cnt
          |from fileinputtable
          |group by id
          |""".stripMargin)

    //7 查询结果的数据
    //7.1 首先需要创建数据对应的虚拟表，输出时，需考虑查询的结果是追加型还是需要更新数据型，需要对用的支持框架
    //7.1.1 文件输出
    val outputPath = "D:\\IdeaWorkSpace\\flink0317\\flink-0317\\src\\main\\resources\\output.txt"
    tableEnv.connect(new FileSystem().path(outputPath))
      .withFormat(new Csv())
      .withSchema(new Schema().field("id",DataTypes.STRING())
        .field("ts",DataTypes.DOUBLE()))
      .createTemporaryTable("fileoutputTable")

    //7.1.2 kafka输出
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      //.properties(props) 可以设置成配置文件，直接传入，也可以如下配置
      .property("zookeeper.connect","localhost:2181")
      .property("bootstrap.servers","localhost:9092"))
      .withFormat(new Csv())
      .withSchema(new Schema().field("id",DataTypes.STRING())
        .field("timestemp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE()))
      .createTemporaryTable("kafkaoutputtable")

    //7.1.3 es输出
    tableEnv.connect(
      new Elasticsearch()
        .version("6")
        .host("localhost", 9200, "http")
        .index("sensor")
        .documentType("temp")
    )
      .inUpsertMode()           // 指定是 Upsert 模式
      .withFormat(new Json())
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
        .field("count", DataTypes.BIGINT())
      )
      .createTemporaryTable("esOutputTable")

    //7.1.4 输出到Mysql，blink版本才能使用
    val sinkDDL: String =
      """
        |create table jdbcOutputTable (
        |  id varchar(20) not null,
        |  cnt bigint not null
        |) with (
        |  'connector.type' = 'jdbc',
        |  'connector.url' = 'jdbc:mysql://localhost:3306/test',
        |  'connector.table' = 'sensor_count',
        |  'connector.driver' = 'com.mysql.jdbc.Driver',
        |  'connector.username' = 'root',
        |  'connector.password' = '123456'
        |)
     """.stripMargin

    tableEnv.sqlUpdate(sinkDDL)

    //7.2 执行输出操作
    resultTable.insertInto("jdbcOutputTable")
    //7.2 更新模式
    /**
     * 1) 追加模式（Append Mode）
     * 在追加模式下，表（动态表）和外部连接器只交换插入（Insert）消息。
     * 2）撤回模式（Retract Mode）
     * 在撤回模式下，表和外部连接器交换的是：添加（Add）和撤回（Retract）消息。
     * 	插入（Insert）会被编码为添加消息；
     * 	删除（Delete）则编码为撤回消息；
     * 	更新（Update）则会编码为，已更新行（上一行）的撤回消息，和更新行（新行）的添加消息。
     * 在此模式下，不能定义key，这一点跟upsert模式完全不同。
     * 3）Upsert（更新插入）模式
     * 在Upsert模式下，动态表和外部连接器交换Upsert和Delete消息。
     * 这个模式需要一个唯一的key，通过这个key可以传递更新消息。为了正确应用消息，外部连接器需要知道这个唯一key的属性。
     * 	插入（Insert）和更新（Update）都被编码为Upsert消息；
     * 	删除（Delete）编码为Delete信息。
     * 这种模式和Retract模式的主要区别在于，Update操作是用单个消息编码的，所以效率会更高。
     */
    //8 table 转换成stream

    resultTable.toAppendStream[Row].print() //追加类型的转换，用toappendStream
    resultTable.toRetractStream[Row].print() //更新类型的转换，用toRetractStream
    /**
     * Table API中表到DataStream有两种模式：
     * 	追加模式（Append Mode）
     * 用于表只会被插入（Insert）操作更改的场景。
     * 	撤回模式（Retract Mode）
     * 用于任何场景。有些类似于更新模式中Retract模式，它只有Insert和Delete两类操作。
     * 得到的数据会增加一个Boolean类型的标识位（返回的第一个字段），用它来表示到底是新增的数据（Insert），还是被删除的数据（老数据， Delete）。
     */

    env.execute()
  }
}

