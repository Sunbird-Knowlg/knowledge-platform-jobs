package org.sunbird.job.dialcodecontextupdater.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.dialcodecontextupdater.domain.Event

import java.util
import scala.collection.JavaConverters._

class DialcodeContextUpdaterConfig(override val config: Config) extends BaseJobConfig(config, "dialcode-context-updater") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val dialcodeContextUpdaterTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaFailedTopic: String = config.getString("kafka.failed.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")
  val dialcodeContextUpdaterParallelism: Int = if (config.hasPath("task.dialcode-context-updater.parallelism"))
    config.getInt("task.dialcode-context-updater.parallelism") else 1

  // Metric List
  val totalEventsCount = "total-message-count"
  val successEventCount = "success-message-count"
  val failedEventCount = "failed-message-count"
  val skippedEventCount = "skipped-message-count"
  val errorEventCount = "error-message-count"

  // Consumers
  val eventConsumer = "dialcode-context-updater-consumer"
  val dialcodeContextUpdaterFunction = "dialcode-context-updater-process"
  val dialcodeContextUpdaterEventProducer = "dialcode-context-updater-producer"

  // Tags
  val dialcodeContextUpdaterOutputTag: OutputTag[Event] = OutputTag[Event]("dialcode-context-updater")
  val failedEventOutTag: OutputTag[String] = OutputTag[String]("dialcode-context-updater-failed-event")

  val configVersion = "1.0"

  // DB Config
  val cassandraHost: String = config.getString("lms-cassandra.host")
  val cassandraPort: Int = config.getInt("lms-cassandra.port")

  val graphRoutePath: String = config.getString("neo4j.routePath")
  val graphName: String = config.getString("neo4j.graph")
  val dbHitEventCount = "db-hit-events-count"

  val searchServiceBaseUrl : String = config.getString("service.search.basePath")
  val dialServiceBaseUrl : String = config.getString("service.dial_service.basePath")
  val dialcodeContextUpdatePath : String = config.getString("dialcode_context_updater.dial_code_context_update_api_path")
  val dialcodeContextReadPath : String = config.getString("dialcode_context_updater.dial_code_context_read_api_path")

  val searchMode: String = if (config.hasPath("dialcode_context_updater.search_mode")) config.getString("dialcode_context_updater.search_mode") else "Collection"
  val contextMapFilePath: String = if (config.hasPath("dialcode_context_updater.context_map_path")) config.getString("dialcode_context_updater.context_map_path") else ""

  val identifierSearchFields: List[String] = if (config.hasPath("dialcode_context_updater.identifier_search_fields")) config.getStringList("dialcode_context_updater.identifier_search_fields").asScala.toList else List("identifier", "primaryCategory","channel")
  val nodeESSyncWaitTime: Int = if (config.hasPath("es_sync_wait_time")) config.getInt("es_sync_wait_time") else 2000
}
