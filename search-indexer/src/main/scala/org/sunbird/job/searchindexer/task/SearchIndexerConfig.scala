package org.sunbird.job.searchindexer.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.searchindexer.compositesearch.domain.Event

import java.util
import scala.collection.JavaConverters._

class SearchIndexerConfig(override val config: Config) extends BaseJobConfig(config, "search-indexer") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Job Configuration
  val jobEnv: String = config.getString("job.env")

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaErrorTopic: String = config.getString("kafka.error.topic")

  // Parallelism
  val eventRouterParallelism: Int = config.getInt("task.router.parallelism")
  val compositeSearchIndexerParallelism: Int = config.getInt("task.compositeSearch.parallelism")
  val dialCodeExternalIndexerParallelism: Int = config.getInt("task.dialcodeIndexer.parallelism")
  val dialCodeMetricIndexerParallelism: Int = config.getInt("task.dialcodemetricsIndexer.parallelism")

  // Consumers
  val searchIndexerConsumer = "search-indexer-consumer"
  val transactionEventRouter = "transaction-event-router"

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
  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap = if (config.hasPath("schema.supportedVersion")) config.getObject("schema.supportedVersion").unwrapped().asScala else Map[String, AnyRef]()
  val definitionCacheExpiry: Int = if (config.hasPath("schema.definition_cache.expiry")) config.getInt("schema.definition_cache.expiry") else 14400
  val restrictObjectTypes: util.List[String] = if(config.hasPath("restrict.objectTypes")) config.getStringList("restrict.objectTypes") else new util.ArrayList[String]
  val ignoredFields: List[String] = if (config.hasPath("ignored.fields")) config.getStringList("ignored.fields").asScala.toList else List("responseDeclaration", "body")
}
