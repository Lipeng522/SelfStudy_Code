package com.flink.table_sql


import com.flink.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Over, Slide, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * table api 和 sql的开窗函数的使用
 *
 */
object TableWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //1 设定时间语义             --->>>>event time 相关设置
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.readTextFile("D:\\IdeaWorkSpace\\flink0317\\flink-0317\\src\\main\\resources\\sensor.txt")
    val sensorStream: DataStream[SensorReading] = inputStream.map(line => {
      val fields: Array[String] = line.split(",")
      SensorReading(fields(0), fields(1).toLong, fields(2).toDouble)
    })
    //2 event time 字段提取及water Mark设置
    val timeStream: DataStream[SensorReading] = sensorStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(500)) {
      override def extractTimestamp(element: SensorReading): Long = element.timeStamp * 1000
    })
    //3.1 设定时间字段的方式,处理时间
    val sensorTable: Table = tableEnv.fromDataStream(sensorStream, 'id, 'temperature as 'temp, 'timeStamp as 'ts, 'pt.proctime)

    //3.2 设定事件时间，先设置为eventtime ------>>>>>event time 相关设置
    val resultTable: Table = tableEnv.fromDataStream(timeStream, 'id, 'timeStamp.rowtime as 'ts, 'temperature as 'temp)

    //4 窗口聚合
    //4.1.1  table API分组窗口
    val windowTable: Table = resultTable.window(Tumble over 10.seconds on 'ts as 'tw) //设置窗口的关联字段是eventtime时间字段
      .groupBy('tw, 'id)
      .select('id, 'id.count as 'cnt, 'tw.start, 'tw.end) //获取窗口的开始时间和结束事假

    //4.1.2 sql 方式
    tableEnv.createTemporaryView("sensor_table",resultTable)
    val sqlTable: Table = tableEnv.sqlQuery(
      """
        |select id,count(id) as cnt,
        |tumble_end(ts,interval '10' second)  //此处使用的tumble_end,滑动窗口的就是hop_end
        |from sensor_table
        |group by id, tumble(ts,interval '10' second)
        |""".stripMargin
    )

    //4.2 开窗函数
    //4.2.1 table API
    val result5Table: Table = resultTable.window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
      .select('id,'ts, 'id.count over 'ow, 'temp.avg over 'ow)

    //4.2.2 sql
    val result6Table: Table = tableEnv.sqlQuery(
      """
        |select id,ts,count(id) over ow,avg(temp) over ow
        |from sensor_table
        |window ow as (
        |partition by id
        |order by ts
        |rows between 2 preceding and current row
        |)
        |""".stripMargin)

/*    //5 其他窗口设置
    val table = resultTable
      .window([w: GroupWindow] as 'w) // 定义窗口，别名 w
      .groupBy('w, 'a)  // 以属性a和窗口w作为分组的key
      .select('a, 'b.sum)  // 聚合字段b的值，求和\
      或者如下
    val table = resultTable
      .window([w: GroupWindow] as 'w)
      .groupBy('w, 'a)
      .select('a, 'w.start, 'w.end, 'w.rowtime, 'b.count)

      //5.1 滚动窗口
      // Tumbling Event-time Window（事件时间字段rowtime）
      .window(Tumble over 10.minutes on 'rowtime as 'w)

      // Tumbling Processing-time Window（处理时间字段proctime）
      .window(Tumble over 10.minutes on 'proctime as 'w)

      // Tumbling Row-count Window (类似于计数窗口，按处理时间排序，10行一组)
      .window(Tumble over 10.rows on 'proctime as 'w)

      //5.2 滑动窗口
      // Sliding Event-time Window
      .window(Slide over 10.minutes every 5.minutes on 'rowtime as 'w)

      // Sliding Processing-time window
      .window(Slide over 10.minutes every 5.minutes on 'proctime as 'w)

      // Sliding Row-count window
      .window(Slide over 10.rows every 5.rows on 'proctime as 'w)

      //5.3 会话窗口
      // Session Event-time Window
      .window(Session withGap 10.minutes on 'rowtime as 'w)

      // Session Processing-time Window
      .window(Session withGap 10.minutes on 'proctime as 'w)
*/

/*    //6 over窗口
    val table = input
      .window([w: OverWindow] as 'w)
    .select('a, 'b.sum over 'w, 'c.min over 'w)

      //6.1 无界的over窗口
      // 无界的事件时间over window (时间字段 "rowtime")
      .window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_RANGE as 'w)

      //无界的处理时间over window (时间字段"proctime")
      .window(Over partitionBy 'a orderBy 'proctime preceding UNBOUNDED_RANGE as 'w)

      // 无界的事件时间Row-count over window (时间字段 "rowtime")
      .window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_ROW as 'w)

      //无界的处理时间Row-count over window (时间字段 "rowtime")
      .window(Over partitionBy 'a orderBy 'proctime preceding UNBOUNDED_ROW as 'w)

      //6.2 有界的over窗口
      // 有界的事件时间over window (时间字段 "rowtime"，之前1分钟)
      .window(Over partitionBy 'a orderBy 'rowtime preceding 1.minutes as 'w)

      // 有界的处理时间over window (时间字段 "rowtime"，之前1分钟)
      .window(Over partitionBy 'a orderBy 'proctime preceding 1.minutes as 'w)

      // 有界的事件时间Row-count over window (时间字段 "rowtime"，之前10行)
      .window(Over partitionBy 'a orderBy 'rowtime preceding 10.rows as 'w)

      // 有界的处理时间Row-count over window (时间字段 "rowtime"，之前10行)
      .window(Over partitionBy 'a orderBy 'proctime preceding 10.rows as 'w)
*/

    //7 groupWindows
    /**
     * SQL支持以下Group窗口函数:
     * 	TUMBLE(time_attr, interval)
     * 定义一个滚动窗口，第一个参数是时间字段，第二个参数是窗口长度。
     * 	HOP(time_attr, interval, interval)
     * 定义一个滑动窗口，第一个参数是时间字段，第二个参数是窗口滑动步长，第三个是窗口长度。
     * 	SESSION(time_attr, interval)
     * 定义一个会话窗口，第一个参数是时间字段，第二个参数是窗口间隔（Gap）。
     *
     * 另外还有一些辅助函数，可以用来选择Group Window的开始和结束时间戳，以及时间属性。
     * 这里只写TUMBLE_*，滑动和会话窗口是类似的（HOP_*，SESSION_*）。
     * 	TUMBLE_START(time_attr, interval)
     * 	TUMBLE_END(time_attr, interval)
     * 	TUMBLE_ROWTIME(time_attr, interval)
     * 	TUMBLE_PROCTIME(time_attr, interval)
     */



    result5Table.printSchema()
    result5Table.toAppendStream[Row].print("result5")
    result6Table.printSchema()
    result6Table.toAppendStream[Row].print("result6")
    env.execute()
  }
}
