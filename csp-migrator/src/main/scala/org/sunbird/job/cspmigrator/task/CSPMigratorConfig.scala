package org.sunbird.job.cspmigrator.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.cspmigrator.domain.Event

import java.util
import scala.collection.JavaConverters._

class CSPMigratorConfig(override val config: Config) extends BaseJobConfig(config, "csp-migrator") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val contentAutoCreatorTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaFailedTopic: String = config.getString("kafka.failed.topic")
  val liveVideoStreamingTopic: String = config.getString("kafka.live_video_stream.topic")
  val liveNodeRepublishTopic: String = config.getString("kafka.live_node_republish.topic")

  val jobEnv: String = config.getString("job.env")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")
  val contentAutoCreatorParallelism: Int = if (config.hasPath("task.csp-migrator.parallelism"))
    config.getInt("task.csp-migrator.parallelism") else 1

  // Metric List
  val totalEventsCount = "total-message-count"
  val successEventCount = "success-message-count"
  val failedEventCount = "failed-message-count"
  val skippedEventCount = "skipped-message-count"
  val errorEventCount = "error-message-count"
  val liveNodePublishCount = "live-node-publish-count"
  val assetVideoStreamCount = "asset-video-stream-count"

  // Consumers
  val eventConsumer = "csp-migrator-consumer"
  val cspMigratorFunction = "csp-migrator-process"
  val cspMigratorEventProducer = "csp-migrator-producer"

  // Tags
  val contentAutoCreatorOutputTag: OutputTag[Event] = OutputTag[Event]("csp-migrator")
  val failedEventOutTag: OutputTag[String] = OutputTag[String]("csp-migrator-failed-event")
  val generateVideoStreamingOutTag: OutputTag[String] = OutputTag[String]("live-video-streaming-generator-request")
  val liveNodePublishEventOutTag: OutputTag[String] = OutputTag[String]("live-node-republish-request")

  val configVersion = "1.0"

  // Cassandra Configurations
  val cassandraHost: String = config.getString("lms-cassandra.host")
  val cassandraPort: Int = config.getInt("lms-cassandra.port")
  val contentKeyspaceName: String = config.getString("content.keyspace")
  val contentTableName: String = config.getString("content.content_table")
  val assessmentTableName: String = config.getString("content.assessment_table")
  val hierarchyKeyspaceName: String = config.getString("hierarchy.keyspace")
  val hierarchyTableName: String = config.getString("hierarchy.table")

  // Neo4J Configurations
  val graphRoutePath: String = config.getString("neo4j.routePath")
  val graphName: String = config.getString("neo4j.graph")

  // Redis Configurations
  val nodeStore: Int = config.getInt("redis.database.contentCache.id")


  // Schema Config
  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap: Map[String, AnyRef] = if (config.hasPath("schema.supportedVersion")) config.getObject("schema.supportedVersion").unwrapped().asScala.toMap else Map[String, AnyRef]()

  val cloudProps: List[String] = if (config.hasPath("object.cloud_props")) config.getStringList("object.cloud_props").asScala.toList else List("variants", "downloadUrl", "appIcon", "posterImage", "pdfUrl")
  val overrideManifestProps: List[String] = if (config.hasPath("object.override_manifest_props")) config.getStringList("object.override_manifest_props").asScala.toList else List("variants", "downloadUrl", "previewUrl", "pdfUrl", "lastPublishedBy")
  val contentServiceBaseUrl : String = config.getString("service.content_service.basePath")
  val searchServiceBaseUrl : String = config.getString("service.search.basePath")
  val learningServiceBaseUrl : String = config.getString("service.learning_service.basePath")

  
  val searchExistsFields: List[String] = if (config.hasPath("search_exists_fields")) config.getStringList("search_exists_fields").asScala.toList else List("originData")
  val searchFields: List[String] = if (config.hasPath("search_fields")) config.getStringList("search_fields").asScala.toList else List("identifier", "mimeType", "pkgVersion", "channel", "status", "origin", "originData", "artifactUrl")

  val contentFolder: String = if (config.hasPath("cloud_storage.folder.content")) config.getString("cloud_storage.folder.content") else "content"
  val artifactFolder: String = if (config.hasPath("cloud_storage.folder.artifact")) config.getString("cloud_storage.folder.artifact") else "artifact"
  val fieldsToMigrate: util.Map[String, AnyRef] = if(config.hasPath("neo4j_fields_to_migrate")) config.getAnyRef("neo4j_fields_to_migrate").asInstanceOf[util.Map[String, AnyRef]] else new util.HashMap[String, AnyRef]()
  val keyValueMigrateStrings: util.Map[String, String] = config.getAnyRef("key_value_strings_to_migrate").asInstanceOf[util.Map[String, String]]

  def getConfig: Config = config
}
