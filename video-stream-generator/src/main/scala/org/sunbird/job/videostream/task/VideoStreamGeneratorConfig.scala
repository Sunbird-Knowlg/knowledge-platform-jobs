package org.sunbird.job.videostream.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.videostream.exception.MediaServiceException

class VideoStreamGeneratorConfig(override val config: Config) extends BaseJobConfig(config, "video-stream-generator") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")

  val timerDuration = config.getInt("task.timer.duration") // Timer duration in sec.
  val maxRetries = if (config.hasPath("task.max.retries")) config.getInt("task.max.retries") else 10

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val retryEventCount = "retry-events-count"
  val skippedEventCount = "skipped-events-count"

  // Consumers
  val videoStreamConsumer = "video-streaming-consumer"
  val videoStreamGeneratorFunction = "manage-streaming-jobs"

  // Cassandra Configurations
  val dbTable: String = config.getString("lms-cassandra.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val hierarchyPrimaryKey: List[String] = List("identifier")

  // LP Configurations
  val lpURL: String = config.getString("service.content.basePath")
  val contentV4Update = "/content/v4/system/update/"

  val jobStatus:util.List[String] = if(config.hasPath("media_service_job_success_status")) config.getStringList("media_service_job_success_status") else util.Arrays.asList("FINISHED", "COMPLETE", "SUCCEEDED")
  val jobFailStatus:util.List[String] = if(config.hasPath("media_service_job_fail_status")) config.getStringList("media_service_job_fail_status") else util.Arrays.asList("ERROR", "FAILED")

  def getConfig(key: String): String = {
    if (config.hasPath(key))
      config.getString(key)
    else throw new MediaServiceException("CONFIG_NOT_FOUND", "Configuration for key [" + key + "] Not Found.")
  }

  def getSystemConfig(key: String): String = {
    val sysKey=key.replaceAll("\\.","_")
    if (config.hasPath(sysKey))
      config.getString(sysKey)
    else throw new MediaServiceException("CONFIG_NOT_FOUND", "Configuration for key [" + sysKey + "] Not Found.")
  }
}
