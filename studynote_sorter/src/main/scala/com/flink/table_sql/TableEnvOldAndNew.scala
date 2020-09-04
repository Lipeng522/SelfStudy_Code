package com.flink.table_sql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala._

/**
 * 针对flink1.10.0，可以选择两种tableEnv，一种是old table的Env，一种是blink的新版Env
 */
object TableEnvOldAndNew {
  def main(args: Array[String]): Unit = {
    //流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //批处理环境
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //1.1 老版本的流处理环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val oldTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //1.2 老版本的批处理环境

    val oldBatchEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)

    //2.1 新版本的流处理环境
    val blinkSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, blinkSettings)

    //2.2 新版本的批处理
    val blinkBatchSetting: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()

    val blinkBatchEnv: TableEnvironment = TableEnvironment.create(blinkBatchSetting)
  }
}
