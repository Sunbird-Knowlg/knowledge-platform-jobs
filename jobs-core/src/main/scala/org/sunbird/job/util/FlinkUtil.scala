package org.sunbird.job.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.RestartStrategyOptions
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.job.BaseJobConfig

import java.time.Duration

object FlinkUtil {

  def getExecutionContext(config: BaseJobConfig): StreamExecutionEnvironment = {
    val flinkConfig = new Configuration()
    flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay")
    flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.valueOf(config.restartAttempts))
    flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(config.delayBetweenAttempts))

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig)
    env.getConfig.setUseSnapshotCompression(config.enableCompressedCheckpointing)
    env.enableCheckpointing(config.checkpointingInterval)
    env.getCheckpointConfig.setCheckpointTimeout(config.checkpointingTimeout)


    /**
     * Use Blob storage as distributed state backend if enabled
     */

    config.enableDistributedCheckpointing match {
      case Some(true) =>
        env.setStateBackend(new HashMapStateBackend())
        val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
        checkpointConfig.setCheckpointStorage(
          new FileSystemCheckpointStorage(s"${config.checkpointingBaseUrl.getOrElse("")}/${config.jobName}")
        )
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        checkpointConfig.setMinPauseBetweenCheckpoints(config.checkpointingPauseSeconds)
      case _ => // Do nothing
    }

    env
  }
}
