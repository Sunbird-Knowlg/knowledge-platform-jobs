package org.sunbird.job.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

class ActivityAggregateUpdaterConfig(override val config: Config) extends BaseJobConfig(config, "activity-aggregate-updater") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaAuditEventTopic: String = config.getString("kafka.output.audit.topic")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val activityAggregateUpdaterParallelism: Int = config.getInt("task.activity.parallelism")

  // Metric List
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val dbUpdateCount = "db-update-count"
  val dbReadCount = "db-read-count"
  val cacheHitCount = "cache-hit-count"
  val cacheMissCount = "cache-miss-count"
  val batchEnrolmentUpdateEventCount = "batch-enrolment-update-count"
  val skipEventsCount = "skipped-events-count"

  // Cassandra Configurations
  val dbUserContentConsumptionTable: String = config.getString("lms-cassandra.consumption.table")
  val dbUserActivityAggTable: String = config.getString("lms-cassandra.user_activity_agg.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")

  // Redis Configurations
  val nodeStore: Int = config.getInt("redis.database.relationCache.id") // Both LeafNodes And Ancestor nodes

  // Tags
  val auditEventOutputTagName = "audit-events"
  val auditEventOutputTag: OutputTag[String] = OutputTag[String](auditEventOutputTagName)

  // constans
  val activityType = "activity_type"
  val activityId = "activity_id"
  val contextId = "context_id"
  val activityUser = "user_id"
  val aggLastUpdated = "agg_last_updated"
  val agg = "agg"
  val courseId = "courseId"
  val batchId = "batchId"
  val contentId = "contentId"
  val progress = "progress"
  val contents = "contents"
  val contentStatus = "contentStatus"
  val userId = "userId"
  val status = "status"
  val unitActivityType = "course-unit"
  val courseActivityType = "course"
  val leafNodes = "leafnodes"
  val ancestors = "ancestors"
  val viewcount = "viewcount"
  val completedcount = "completedcount"
  val complete = "complete"
  val eData = "edata"
  val action = "action"
  val batchEnrolmentUpdateCode = "batch-enrolment-update"
  val routerFn = "RouterFn"
  val activityAggregateUpdaterFn = "activity-aggregate-updater-fn"
  val partition = "partition"
  val courseBatch = "CourseBatch"

  // Consumers
  val activityAggregateUpdaterConsumer = "activity-aggregate-updater-consumer"

  // Producers
  val activityAggregateUpdaterProducer = "activity-aggregate-updater-audit-events-sink"

  //Thresholds
  val thresholdBatchReadInterval: Int = config.getInt("threshold.batch.read.interval")
  val thresholdBatchReadSize: Int = config.getInt("threshold.batch.read.size")
  val thresholdBatchWriteSize: Int = config.getInt("threshold.batch.write.size")

}
