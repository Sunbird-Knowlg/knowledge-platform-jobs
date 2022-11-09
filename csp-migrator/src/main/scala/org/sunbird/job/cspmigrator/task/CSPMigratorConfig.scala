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

  val contentServiceBaseUrl : String = config.getString("service.content_service.basePath")
  val searchServiceBaseUrl : String = config.getString("service.search.basePath")
  val learningServiceBaseUrl : String = config.getString("service.learning_service.basePath")

  
  val searchExistsFields: List[String] = if (config.hasPath("search_exists_fields")) config.getStringList("search_exists_fields").asScala.toList else List("originData")
  val searchFields: List[String] = if (config.hasPath("search_fields")) config.getStringList("search_fields").asScala.toList else List("identifier", "mimeType", "pkgVersion", "channel", "status", "origin", "originData", "artifactUrl")

  val fieldsToMigrate: util.Map[String, AnyRef] = if(config.hasPath("neo4j_fields_to_migrate")) config.getAnyRef("neo4j_fields_to_migrate").asInstanceOf[util.Map[String, AnyRef]] else new util.HashMap[String, AnyRef]()
  val keyValueMigrateStrings: util.Map[String, String] = config.getAnyRef("key_value_strings_to_migrate").asInstanceOf[util.Map[String, String]]
  val migrationVersion: Int = if(config.hasPath("migrationVersion"))  config.getInt("migrationVersion") else 1

  val videStreamRegenerationEnabled: Boolean = if(config.hasPath("video_stream_regeneration_enable"))  config.getBoolean("video_stream_regeneration_enable") else true
  val liveNodeRepublishEnabled: Boolean = if(config.hasPath("live_node_republish_enable"))  config.getBoolean("live_node_republish_enable") else true

  def getConfig: Config = config
}
