package org.sunbird.job.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.domain.CollectionProgress

class EnrolmentReconciliationConfig(override val config: Config) extends BaseJobConfig(config, "enrolment-reconciliation") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val enrolmentCompleteTypeInfo: TypeInformation[List[CollectionProgress]] = TypeExtractor.getForClass(classOf[List[CollectionProgress]])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaAuditEventTopic: String = config.getString("kafka.output.audit.topic")
  val kafkaFailedEventTopic: String = config.getString("kafka.output.failed.topic")
  val kafkaCertIssueTopic: String = config.getString("kafka.output.certissue.topic")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val enrolmentReconciliationParallelism: Int = config.getInt("task.enrolment.reconciliation.parallelism")
  val deDupProcessParallelism: Int = config.getInt("task.dedup.parallelism")
  val enrolmentCompleteParallelism: Int = config.getInt("task.enrolment.complete.parallelism")

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


  // Producers
  val enrolmentReconciliationProducer = "enrolment-reconciliation-audit-events-sink"
  val enrolmentCompleteEventProducer = "enrolment-complete-audit-sink"
  val enrolmentReconciliationFailedEventProducer = "enrolment-reconciliation-failed-sink"
  val certIssueEventProducer = "certificate-issue-event-producer"

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

  // Other services configuration
  val searchServiceBasePath: String = config.getString("service.search.basePath")
  val searchAPIURL = searchServiceBasePath + "/v3/search"

  val auditEventOutputTagName = "audit-events"
  val auditEventOutputTag: OutputTag[String] = OutputTag[String](auditEventOutputTagName)
  val failedEventOutputTagName = "failed-events"
  val failedEventOutputTag: OutputTag[String] = OutputTag[String](failedEventOutputTagName)
  val collectionCompleteOutputTagName = "collection-progress-complete-events"
  val collectionCompleteOutputTag: OutputTag[List[CollectionProgress]] = OutputTag[List[CollectionProgress]](collectionCompleteOutputTagName)
  val collectionUpdateOutputTagName = "collection-progress-update-events"
  val collectionUpdateOutputTag: OutputTag[List[CollectionProgress]] = OutputTag[List[CollectionProgress]](collectionUpdateOutputTagName)
  val certIssueOutputTagName = "certificate-issue-events"
  val certIssueOutputTag: OutputTag[String] = OutputTag[String](certIssueOutputTagName)
  
  // Job specific configurations
  val thresholdBatchWriteSize: Int = config.getInt("threshold.batch.write.size")
  val moduleAggEnabled: Boolean = config.getBoolean("activity.module.aggs.enabled")
  val dedupEnabled: Boolean = config.getBoolean("activity.input.dedup.enabled")
  val statusCacheExpirySec: Int = config.getInt("activity.collection.status.cache.expiry")
  val filterCompletedEnrolments: Boolean =  if (config.hasPath("activity.filter.processed.enrolments")) config.getBoolean("activity.filter.processed.enrolments") else true

  // constants
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
  val consumptionDeDupFn= "consumption-dedup-process"
  val activityAggregateUpdaterFn = "activity-aggregate-updater-fn"
  val partition = "partition"
  val courseBatch = "CourseBatch"
  val collectionProgressUpdateFn = "progress-update-process"
  val collectionCompleteFn = "collection-completion-process"


}
