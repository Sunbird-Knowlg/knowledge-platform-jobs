package org.sunbird.job.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.BaseJobConfig

class EnrolmentReconciliationConfig(override val config: Config) extends BaseJobConfig(config, "enrolment-reconciliation") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  // Metric List
  val totalEventCount = "total-events-count"
  val failedEventCount = "failed-events-count"
  val dbUpdateCount = "db-update-count"
  val dbReadCount = "db-read-count"
  val cacheHitCount = "cache-hit-count"
  val cacheMissCount = "cache-miss-count"
  val skipEventsCount = "skipped-events-count"
  val certIssueEventsCount = "cert-issue-events-count"
  val processedEnrolmentCount = "processed-enrolment-count"
  val enrolmentCompleteCount = "enrolment-complete-count"
  val retiredCCEventsCount = "retired-consumption-events-count"

  // Consumers
  val enrolmentReconciliationConsumer = "enrolment-reconciliation-consumer"

  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val dbUserContentConsumptionTable: String = config.getString("lms-cassandra.consumption.table")
  val dbUserActivityAggTable: String = config.getString("lms-cassandra.user_activity_agg.table")
  val dbUserEnrolmentsTable: String = config.getString("lms-cassandra.user_enrolments.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")



  // Redis Configurations
  val nodeStore: Int = config.getInt("redis.database.relationCache.id") // Both LeafNodes And Ancestor nodes

  val deDupRedisHost: String = config.getString("dedup-redis.host")
  val deDupRedisPort: Int = config.getInt("dedup-redis.port")
  val deDupStore: Int = config.getInt("dedup-redis.database.index")
  val deDupExpirySec: Int = config.getInt("dedup-redis.database.expiry")

  val supportedEventType:String = "user-enrolment-sync"
  val statusCacheExpirySec: Int = 10


}
