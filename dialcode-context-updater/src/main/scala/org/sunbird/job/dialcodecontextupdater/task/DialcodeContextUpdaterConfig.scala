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
  val cassandraDialCodeKeyspace: String = config.getString("cassandra.dialcode_keyspace")
  val cassandraDialCodeTable: String = config.getString("cassandra.dialcode_table")
//  val cassandraHierarchyKeyspace: String = config.getString("cassandra.hierarchy_keyspace")
//  val cassandraHierarchyTable: String = config.getString("cassandra.hierarchy_table")

  val graphRoutePath: String = config.getString("neo4j.routePath")
  val graphName: String = config.getString("neo4j.graph")
  val dbHitEventCount = "db-hit-events-count"

  // Schema Config
  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap: Map[String, AnyRef] = if (config.hasPath("schema.supportedVersion")) config.getObject("schema.supportedVersion").unwrapped().asScala.toMap else Map[String, AnyRef]()

  val overrideManifestProps: List[String] = if (config.hasPath("object.override_manifest_props")) config.getStringList("object.override_manifest_props").asScala.toList else List("variants", "downloadUrl", "previewUrl", "pdfUrl", "lastPublishedBy")
  val contentServiceBaseUrl : String = config.getString("service.content_service.basePath")
  val searchServiceBaseUrl : String = config.getString("service.search.basePath")
  val learningServiceBaseUrl : String = config.getString("service.learning_service.basePath")

  val searchMode: String = if (config.hasPath("search_mode")) config.getString("search_mode") else "Collection"
  val contextMapFilePath: String = if (config.hasPath("context_map_path")) config.getString("context_map_path") else ""

  val contentFolder: String = if (config.hasPath("cloud_storage.folder.content")) config.getString("cloud_storage.folder.content") else "content"
  val artifactFolder: String = if (config.hasPath("cloud_storage.folder.artifact")) config.getString("cloud_storage.folder.artifact") else "artifact"

  val apiCallDelay: Int = if (config.hasPath("content_auto_creator.api_call_delay")) config.getInt("content_auto_creator.api_call_delay") else 2
  val identifierSearchFields: List[String] = if (config.hasPath("identifier_search_fields")) config.getStringList("identifier_search_fields").asScala.toList else List("identifier", "primaryCategory")

  def getConfig: Config = config
}
