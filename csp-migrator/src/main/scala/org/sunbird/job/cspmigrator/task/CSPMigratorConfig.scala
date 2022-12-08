package org.sunbird.job.cspmigrator.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.cspmigrator.domain.Event

import java.util

class CSPMigratorConfig(override val config: Config) extends BaseJobConfig(config, "csp-migrator") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val contentAutoCreatorTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaFailedTopic: String = config.getString("kafka.failed.topic")
  val liveVideoStreamingTopic: String = config.getString("kafka.live_video_stream.topic")
  val liveContentNodeRepublishTopic: String = config.getString("kafka.live_content_node_republish.topic")
  val liveQuestionNodeRepublishTopic: String = config.getString("kafka.live_question_node_republish.topic")

  val jobEnv: String = config.getString("job.env")

  val cassandraMigrationOutputTag: OutputTag[Event] = OutputTag[Event]("csp-cassandra-migration")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")
  val cassandraMigratorParallelism: Int = if (config.hasPath("task.cassandra-migrator.parallelism"))
    config.getInt("task.cassandra-migrator.parallelism") else 1

  // Metric List
  val totalEventsCount = "csp-total-message-count"
  val successEventCount = "csp-success-message-count"
  val failedEventCount = "csp-failed-message-count"
  val skippedEventCount = "csp-skipped-message-count"
  val errorEventCount = "csp-error-message-count"
  val liveContentNodePublishCount = "live-content-node-publish-count"
  val assetVideoStreamCount = "asset-video-stream-count"
  val liveQuestionNodePublishCount = "live-question-node-publish-count"
  val liveQuestionSetNodePublishCount = "live-questionset-node-publish-count"

  // Consumers
  val eventConsumer = "csp-migrator-consumer"
  val cspMigratorFunction = "csp-migrator-process"
  val cspMigratorEventProducer = "csp-migrator-producer"
  val cassandraMigratorFunction = "csp-cassandra-migrator-process"

  // Tags
  val contentAutoCreatorOutputTag: OutputTag[Event] = OutputTag[Event]("csp-migrator")
  val failedEventOutTag: OutputTag[String] = OutputTag[String]("csp-migrator-failed-event")
  val generateVideoStreamingOutTag: OutputTag[String] = OutputTag[String]("live-video-streaming-generator-request")
  val liveContentNodePublishEventOutTag: OutputTag[String] = OutputTag[String]("live-content-node-republish-request")
  val liveQuestionSetNodePublishEventOutTag: OutputTag[String] = OutputTag[String]("live-questionset-node-republish-request")
  val liveQuestionNodePublishEventOutTag: OutputTag[String] = OutputTag[String]("live-question-node-republish-request")
  val liveCollectionNodePublishEventOutTag: OutputTag[String] = OutputTag[String]("live-collection-node-republish-request")

  val configVersion = "1.0"

  // Cassandra Configurations
  val cassandraHost: String = config.getString("lms-cassandra.host")
  val cassandraPort: Int = config.getInt("lms-cassandra.port")
  val contentKeyspaceName: String = config.getString("content.keyspace")
  val contentTableName: String = config.getString("content.content_table")
  val assessmentTableName: String = config.getString("content.assessment_table")
  val hierarchyKeyspaceName: String = config.getString("hierarchy.keyspace")
  val hierarchyTableName: String = config.getString("hierarchy.table")
  val qsHierarchyKeyspaceName: String = config.getString("questionset.hierarchy.keyspace")
  val qsHierarchyTableName: String = config.getString("questionset.hierarchy.table")

  // Neo4J Configurations
  val graphRoutePath: String = config.getString("neo4j.routePath")
  val graphName: String = config.getString("neo4j.graph")

  val fieldsToMigrate: util.Map[String, AnyRef] = if(config.hasPath("neo4j_fields_to_migrate")) config.getAnyRef("neo4j_fields_to_migrate").asInstanceOf[util.Map[String, AnyRef]] else new util.HashMap[String, AnyRef]()
  val keyValueMigrateStrings: util.Map[String, String] = config.getAnyRef("key_value_strings_to_migrate").asInstanceOf[util.Map[String, String]]
  val migrationVersion: Double = if(config.hasPath("migrationVersion"))  config.getDouble("migrationVersion") else 1.0

  val videStreamRegenerationEnabled: Boolean = if(config.hasPath("video_stream_regeneration_enable"))  config.getBoolean("video_stream_regeneration_enable") else true
  val liveNodeRepublishEnabled: Boolean = if(config.hasPath("live_node_republish_enable"))  config.getBoolean("live_node_republish_enable") else true
  val copyMissingFiles: Boolean = if(config.hasPath("copy_missing_files_to_cloud"))  config.getBoolean("copy_missing_files_to_cloud") else true

  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"

  def getConfig: Config = config
}
