package org.sunbird.job.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.exception.MediaServiceException

class VideoStreamGeneratorConfig(override val config: Config) extends BaseJobConfig(config, "video-stream-generator") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  val windowTime = config.getInt("task.window.time")

  // Metric List
  val totalEventsCount = "total-events-count"
  val skippedEventCount = "skipped-event-count"
  val cacheWrite = "cache-write-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val dbUpdateCount = "db-update-count"
  val dbReadCount = "db-read-count"
  val cacheHitCount = "cache-hit-count"
  val cacheMissCount = "cache-miss-count"
  val batchEnrolmentUpdateEventCount = "batch-enrolment-update-count"
  val skipEventsCount = "skipped-events-count"

  // Consumers
  val videoStreamConsumer = "video-stream-generator-consumer"
  val videoStreamUrlUpdatorConsumer = "video-stream-url-updator-consumer"

  // Tags
  val videoStreamJobStatusTagName = "video-stream-job-status"
  val videoStreamJobOutput: OutputTag[String] = OutputTag[String](videoStreamJobStatusTagName)

  // Cassandra Configurations
  val dbTable: String = config.getString("lms-cassandra.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val hierarchyPrimaryKey: List[String] = List("identifier")

  // Redis Configurations
  val relationCacheStore: Int = config.getInt("redis.database.relationCache.id")
  val collectionCacheStore: Int = config.getInt("redis.database.collectionCache.id")

  // LP Configurations
  val lpURL: String = config.getString("lp.url")
  val contentV3Update = "/content/v3/update/"

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
