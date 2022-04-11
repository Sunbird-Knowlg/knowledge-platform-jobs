package org.sunbird.job.contentautocreator.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.contentautocreator.domain.Event

import java.util
import scala.collection.JavaConverters._

class ContentAutoCreatorConfig(override val config: Config) extends BaseJobConfig(config, "content-auto-creator") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val contentAutoCreatorTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaFailedTopic: String = config.getString("kafka.failed.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")
  val contentAutoCreatorParallelism: Int = if (config.hasPath("task.content-auto-creator.parallelism"))
    config.getInt("task.content-auto-creator.parallelism") else 1

  // Metric List
  val totalEventsCount = "total-message-count"
  val successEventCount = "success-message-count"
  val failedEventCount = "failed-message-count"
  val skippedEventCount = "skipped-message-count"
  val errorEventCount = "error-message-count"

  // Consumers
  val eventConsumer = "content-auto-creator-consumer"
  val contentAutoCreatorFunction = "content-auto-creator-process"
  val contentAutoCreatorEventProducer = "content-auto-creator-producer"

  // Tags
  val contentAutoCreatorOutputTag: OutputTag[Event] = OutputTag[Event]("content-auto-creator")
  val failedEventOutTag: OutputTag[String] = OutputTag[String]("content-auto-creator-failed-event")

  val configVersion = "1.0"

  // DB Config
  val cassandraHost: String = config.getString("lms-cassandra.host")
  val cassandraPort: Int = config.getInt("lms-cassandra.port")
  val graphRoutePath: String = config.getString("neo4j.routePath")
  val graphName: String = config.getString("neo4j.graph")


  // Schema Config
  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap: Map[String, AnyRef] = if (config.hasPath("schema.supportedVersion")) config.getObject("schema.supportedVersion").unwrapped().asScala.toMap else Map[String, AnyRef]()

  val cloudProps: List[String] = if (config.hasPath("object.cloud_props")) config.getStringList("object.cloud_props").asScala.toList else List("variants", "downloadUrl", "appIcon", "posterImage", "pdfUrl")
  val overrideManifestProps: List[String] = if (config.hasPath("object.override_manifest_props")) config.getStringList("object.override_manifest_props").asScala.toList else List("variants", "downloadUrl", "previewUrl", "pdfUrl", "lastPublishedBy")
  val contentServiceBaseUrl : String = config.getString("service.content_service.basePath")
  val searchServiceBaseUrl : String = config.getString("service.search.basePath")
  val learningServiceBaseUrl : String = config.getString("service.learning_service.basePath")

  val allowedContentStages: List[String] = if (config.hasPath("content_auto_creator.allowed_content_stages")) config.getStringList("content_auto_creator.allowed_content_stages").asScala.toList else List("create", "upload", "review", "publish")
  val allowedContentObjectTypes: List[String] = if (config.hasPath("content_auto_creator.allowed_object_types")) config.getStringList("content_auto_creator.allowed_object_types").asScala.toList else List("Content")
  val artifactAllowedSources: List[String] = if (config.hasPath("content_auto_creator.artifact_upload_allowed_source")) config.getStringList("content_auto_creator.artifact_upload_allowed_source").asScala.toList else List.empty
  val mandatoryContentMetadata: List[String] = if (config.hasPath("content_auto_creator.content_mandatory_fields")) config.getStringList("content_auto_creator.content_mandatory_fields").asScala.toList else List.empty
  val content_props_to_removed : List[String] = if (config.hasPath("content_auto_creator.content_props_to_removed")) config.getStringList("content_auto_creator.content_props_to_removed").asScala.toList else List.empty
  val content_create_props : List[String] = if (config.hasPath("content_auto_creator.content_create_props")) config.getStringList("content_auto_creator.content_create_props").asScala.toList else List.empty
  val temp_file_location: String = if (config.hasPath("content_auto_creator.temp_file_location")) config.getString("content_auto_creator.temp_file_location") else "/tmp/content"
  val artifactMaxSize: Long = if (config.hasPath("content_auto_creator.artifact_upload_max_size")) config.getLong("content_auto_creator.artifact_upload_max_size") else 62914560
  val bulkUploadMimeTypes: List[String] = if (config.hasPath("content_auto_creator.bulk_upload_mime_types")) config.getStringList("content_auto_creator.bulk_upload_mime_types").asScala.toList else List.empty

  val maxIteration: Int = config.getInt("content_auto_creator.maxIteration")

  val searchExistsFields: List[String] = if (config.hasPath("search_exists_fields")) config.getStringList("search_exists_fields").asScala.toList else List("originData")
  val searchFields: List[String] = if (config.hasPath("search_fields")) config.getStringList("search_fields").asScala.toList else List("identifier", "mimeType", "pkgVersion", "channel", "status", "origin", "originData", "artifactUrl")

  val contentFolder: String = if (config.hasPath("cloud_storage.folder.content")) config.getString("cloud_storage.folder.content") else "content"
  val artifactFolder: String = if (config.hasPath("cloud_storage.folder.artifact")) config.getString("cloud_storage.folder.artifact") else "artifact"

  val apiCallDelay = if (config.hasPath("content_auto_creator.api_call_delay")) config.getInt("content_auto_creator.api_call_delay") else 2

  def getConfig: Config = config
}
