package org.sunbird.job.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.autocreatorv2.model.ObjectParent

import scala.collection.JavaConverters._

class AutoCreatorV2Config(override val config: Config) extends BaseJobConfig(config, "auto-creator-v2") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val objectParentTypeInfo: TypeInformation[ObjectParent] = TypeExtractor.getForClass(classOf[ObjectParent])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")
  val linkCollectionParallelism: Int = if (config.hasPath("task.link-collection.parallelism"))
    config.getInt("task.link-collection.parallelism") else 1

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-events-count"

  // Consumers
  val eventConsumer = "auto-creator-v2-consumer"
  val autoCreatorV2Function = "auto-creator-v2"
  val autoCreatorEventProducer = "auto-creator-v2-producer"
  val linkCollectionFunction = "link-collection-process"

  // Tags
  val linkCollectionOutputTag: OutputTag[ObjectParent] = OutputTag[ObjectParent]("link-collection")

  val configVersion = "1.0"

  // DB Config
  val cassandraHost: String = config.getString("lms-cassandra.host")
  val cassandraPort: Int = config.getInt("lms-cassandra.port")
  val graphRoutePath = config.getString("neo4j.routePath")
  val graphName = config.getString("neo4j.graph")


  // Schema Config
  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap = if (config.hasPath("schema.supportedVersion")) config.getObject("schema.supportedVersion").unwrapped().asScala.toMap else Map[String, AnyRef]()

  val cloudProps: List[String] = if (config.hasPath("object.cloud_props")) config.getStringList("object.cloud_props").asScala.toList else List("variants", "downloadUrl", "appIcon", "posterImage", "pdfUrl")
  val overrideManifestProps: List[String] = if (config.hasPath("object.override_manifest_props")) config.getStringList("object.override_manifest_props").asScala.toList else List("variants", "downloadUrl", "previewUrl", "pdfUrl", "lastPublishedBy")
  val expandableObjects: List[String] = if (config.hasPath("expandable_objects")) config.getStringList("expandable_objects").asScala.toList else List("QuestionSet")
  val nonExpandableObjects: List[String] = if (config.hasPath("non_expandable_objects")) config.getStringList("non_expandable_objects").asScala.toList else List("Question")
  val graphEnabledObjects: List[String] = if (config.hasPath("graph_enabled_objecttypes")) config.getStringList("graph_enabled_objecttypes").asScala.toList else List("Question")
  val contentServiceBaseUrl : String = config.getString("service.content.basePath")
  val sourceBaseUrl: String = config.getString("source.baseUrl")

  def getString(key: String, default: String): String = {
    if(config.hasPath(key)) config.getString(key) else default
  }

  def getConfig() = config
}
