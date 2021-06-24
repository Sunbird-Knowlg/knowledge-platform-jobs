package org.sunbird.job.util

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.BaseJobConfig

object FlinkUtil {

  def getExecutionContext(config: BaseJobConfig): StreamExecutionEnvironment = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setUseSnapshotCompression(config.enableCompressedCheckpointing)
    env.enableCheckpointing(config.checkpointingInterval)
    env.getCheckpointConfig.setCheckpointTimeout(config.checkpointingTimeout)
    

    /**
     * Use Blob storage as distributed state backend if enabled
     */

    config.enableDistributedCheckpointing match {
      case Some(true) => {
        val stateBackend: StateBackend = new FsStateBackend(s"${config.checkpointingBaseUrl.getOrElse("")}/${config.jobName}", true)
        env.setStateBackend(stateBackend)
        val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
        checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        checkpointConfig.setMinPauseBetweenCheckpoints(config.checkpointingPauseSeconds)
      }
      case _ => // Do nothing
    }

    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(config.restartAttempts, config.delayBetweenAttempts))
    env
  }
}