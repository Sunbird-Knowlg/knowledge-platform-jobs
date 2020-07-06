package org.sunbird.kp.course.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.async.core.BaseJobConfig

class CourseMetricsAggregatorConfig(override val config: Config) extends BaseJobConfig(config, "course-metrics-aggregator") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaAuditEventTopic: String = config.getString("kafka.output.audit.topic")
  val eventMaxSize: Long = config.getLong("kafka.event.max.size")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val progressUpdaterParallelism: Int = config.getInt("task.progressUpdater.parallelism")
  val routerParallelism: Int = config.getInt("task.router.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val dbUpdateCount = "db-update-count"
  val dbReadCount = "db-read-count"
  val skippedEventCount = "skipped-event-count"
  val cacheHitCount = "cache-hit-count"
  val batchEnrolmentUpdateEventCount = "batch-enrolment-update-count"

  // Cassandra Configurations
  val dbContentConsumptionTable: String = config.getString("lms-cassandra.consumption.table")
  val dbUserActivityAggTable: String = config.getString("lms-cassandra.activity_user_agg.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")

  // Redis Configurations
  val nodeStore: Int = config.getInt("redis.database.nodes.id") // Both LeafNodes And Ancestor nodes

  // Tags
  val failedEventOutputTagName = "failed-events"
  val successEventOutputTagName = "success-events"
  val auditEventOutputTagName = "audit-events"
  val batchEnrolmentOutputTagName = "batch-enrolment-update"

  val successEventOutputTag: OutputTag[String] = OutputTag[String](successEventOutputTagName)
  val failedEventsOutputTag: OutputTag[String] = OutputTag[String](failedEventOutputTagName)
  val auditEventOutputTag: OutputTag[String] = OutputTag[String](auditEventOutputTagName)
  val batchEnrolmentUpdateOutputTag: OutputTag[util.Map[String, AnyRef]] = OutputTag[util.Map[String, AnyRef]](batchEnrolmentOutputTagName)

  // Consumers
  val courseMetricsUpdaterConsumer = "course-metrics-updater-consumer"


  // Producers
  val courseMetricsAuditProducer = "course-audit-events-sink"

  val completedStatusCode: Int = 2
  val inCompleteStatusCode: Int = 1
  val completionPercentage: Int = 100
  val primaryFields = List("userid", "courseid", "batchid")

  // constans
  val activityType = "activity_type"
  val activityId = "activity_id"
  val contextId = "context_id"
  val activityUser = "user_id"
  val aggLastUpdated = "agg_last_updated"
  val agg = "agg"
  val courseId = "courseid"
  val batchId = "batchid"
  val contentId = "contentid"
  val progress = "progress"
  val userId = "userid"
  val unitActivityType = "course-unit"
  val courseActivityType = "course"
  val leafNodes = "leafnodes"
  val ancestors = "ancestors"

  val windowTimingInSec: Int = config.getInt("window.period")
  val maxQueryBatchSize:Int = config.getInt("query.batch.size")

  val routerFn = "RouterFn"
  val ProgressUpdaterFn = "ProgressUpdaterFn"


}
