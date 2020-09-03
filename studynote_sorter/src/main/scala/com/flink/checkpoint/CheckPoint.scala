package com.flink.checkpoint

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CheckPoint {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new RocksDBStateBackend("")) //设置checkpoint的策略

    //checkpoint容错机制相关配置
    env.enableCheckpointing(1000L) //检查点设置的间隔（source端）
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //一致性策略
    env.getCheckpointConfig.setCheckpointTimeout(60000L) //检查点超时时间
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L) // 检查点之间的最小间隔（sink结束端）
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true) //设置成从检查点恢复数据
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3) //设置可容忍的检查点失败次数

    // 重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 10000L)) //一般选用
    //    env.setRestartStrategy(RestartStrategies.noRestart())

  }
}
