package org.sunbird.job.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.compositesearch.domain.Event
import scala.collection.JavaConverters._

class SearchIndexerConfig(override val config: Config) extends BaseJobConfig(config, "composite-search-indexer") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Job Configuration
  val jobEnv: String = config.getString("job.env")

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaErrorTopic: String = config.getString("kafka.error.topic")

  // Parallelism
  val eventRouterParallelism: Int = config.getInt("task.router.parallelism")
  val compositeSearchIndexerParallelism: Int = config.getInt("task.composite_search.parallelism")
  val dialCodeExternalIndexerParallelism: Int = config.getInt("task.dialcode_external.parallelism")
  val dialCodeMetricIndexerParallelism: Int = config.getInt("task.dialcode_metric.parallelism")

  // Consumers
  val compositeSearchIndexerConsumer = "composite-search-indexer-consumer"
  val compositeSearchIndexerRouter = "composite-search-indexer"

  // Metric List
  val totalEventsCount = "total-events-count"
  val skippedEventCount = "skipped-event-count"
  val compositeSearchEventCount = "composite-search-event-count"
  val successCompositeSearchEventCount = "composite-search-event-success-count"
  val failedCompositeSearchEventCount = "composite-search-event-failed-count"
  val dialcodeMetricEventCount = "dialcode-metric-event-count"
  val successDialcodeMetricEventCount = "dialcode-metric-event-success-count"
  val failedDialcodeMetricEventCount = "dialcode-metric-event-failed-count"
  val dialcodeExternalEventCount = "dialcode-external-event-count"
  val successDialcodeExternalEventCount = "dialcode-external-event-success-count"
  val failedDialcodeExternalEventCount = "dialcode-external-event-failed-count"

  // Tags
  val compositeSearchDataOutTag: OutputTag[Event] = OutputTag[Event]("composite-search-data")
  val dialCodeExternalOutTag: OutputTag[Event] = OutputTag[Event]("dialcode-external")
  val dialCodeMetricOutTag: OutputTag[Event] = OutputTag[Event]("dialcode-metric")
  val failedEventOutTag: OutputTag[String] = OutputTag[String]("failed-event")

  // ES Configs
  val esConnectionInfo = config.getString("es.basePath")

  val compositeSearchIndex: String = if (config.hasPath("compositesearch.index.name")) config.getString("compositesearch.index.name") else "compositesearch"
  val compositeSearchIndexType: String = "cs"

  val dialcodeExternalIndex: String = if (config.hasPath("dialcode.index.name")) config.getString("dialcode.index.name") else "dialcode"
  val dialcodeExternalIndexType: String = "dc"

  val dialcodeMetricIndex: String = if (config.hasPath("dailcodemetrics.index.name")) config.getString("dailcodemetrics.index.name") else "dialcodemetrics"
  val dialcodeMetricIndexType: String = "dcm"

  val restrictMetadataObjectTypes: util.List[String] = if (config.hasPath("restrict.metadata.objectTypes")) config.getStringList("restrict.metadata.objectTypes") else new util.ArrayList[String]
  val nestedFields: util.List[String] = if (config.hasPath("nested.fields")) config.getStringList("nested.fields") else new util.ArrayList[String]
  val definitionBasePath: String = if (config.hasPath("schema.base_path")) config.getString("schema.base_path") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap = if (config.hasPath("schema.supported_version")) config.getAnyRef("schema.supported_version").asInstanceOf[util.Map[String, String]].asScala.toMap else Map[String, String]()
  val definitionCacheExpiry: Long = if (config.hasPath("schema.definition_cache.expiry")) config.getLong("schema.definition_cache.expiry") else 600000.toLong
}
