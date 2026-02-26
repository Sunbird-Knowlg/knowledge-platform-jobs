package org.sunbird.job.transaction.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.transaction.domain.Event
import scala.collection.JavaConverters._

class TransactionEventProcessorConfig(override val config: Config)
    extends BaseJobConfig(config, "transaction-event-processor") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] =
    TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] =
    TypeExtractor.getForClass(classOf[String])
  implicit val eventTypeInfo: TypeInformation[Event] =
    TypeExtractor.getForClass(classOf[Event])

  // Job Configuration
  val jobEnv: String = config.getString("job.env")

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaAuditOutputTopic: String =
    config.getString("kafka.output.audit.topic")
  val kafkaObsrvOutputTopic: String =
    config.getString("kafka.output.obsrv.topic")
  val kafkaErrorTopic: String = config.getString("kafka.error.topic")
  override val kafkaConsumerParallelism: Int =
    config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")

  // Parallelism
  val eventRouterParallelism: Int = config.getInt("task.router.parallelism")
  val compositeSearchIndexerParallelism: Int =
    config.getInt("task.compositeSearch.parallelism")
  val dialCodeExternalIndexerParallelism: Int =
    config.getInt("task.dialcodeIndexer.parallelism")
  val dialCodeMetricIndexerParallelism: Int =
    config.getInt("task.dialcodemetricsIndexer.parallelism")

  // Flink Jobs Configuration
  val kafkaProducerParallelism: Int = config.getInt("task.producer.parallelism")
  val auditEventGenerator: Boolean =
    if (config.hasPath("job.audit-event-generator"))
      config.getBoolean("job.audit-event-generator")
    else true
  val auditHistoryIndexer: Boolean =
    if (config.hasPath("job.audit-history-indexer"))
      config.getBoolean("job.audit-history-indexer")
    else false
  val obsrvMetadataGenerator: Boolean =
    if (config.hasPath("job.obsrv-metadata-generator"))
      config.getBoolean("job.obsrv-metadata-generator")
    else false
  val compositeSearchIndexer: Boolean =
    if (config.hasPath("job.composite-search-indexer"))
      config.getBoolean("job.composite-search-indexer")
    else true
  val dialCodeIndexer: Boolean =
    if (config.hasPath("job.dialcode-indexer"))
      config.getBoolean("job.dialcode-indexer")
    else true
  val dialCodeMetricsIndexer: Boolean =
    if (config.hasPath("job.dialcode-metrics-indexer"))
      config.getBoolean("job.dialcode-metrics-indexer")
    else true

  val outputTag: OutputTag[Event] = OutputTag[Event]("output-tag")
  val auditOutputTag: OutputTag[String] = OutputTag[String]("audit-event-tag")
  val obsrvAuditOutputTag: OutputTag[String] =
    OutputTag[String]("obsrv-metadata-tag")
  val compositeSearchDataOutTag: OutputTag[Event] =
    OutputTag[Event]("composite-search-data")
  val dialCodeExternalOutTag: OutputTag[Event] =
    OutputTag[Event]("dialcode-external")
  val dialCodeMetricOutTag: OutputTag[Event] =
    OutputTag[Event]("dialcode-metric")
  val failedEventOutTag: OutputTag[String] = OutputTag[String]("failed-event")

  val defaultChannel: String = config.getString("channel.default")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-events-count"
  val emptySchemaEventCount = "empty-schema-events-count"
  val emptyPropsEventCount = "empty-props-events-count"
  val esFailedEventCount = "elasticsearch-error-events-count"

  // Search indexer Metric List
  val compositeSearchEventCount = "composite-search-event-count"
  val successCompositeSearchEventCount = "composite-search-event-success-count"
  val failedCompositeSearchEventCount = "composite-search-event-failed-count"

  // DIAL Code Metric Metric List
  val dialcodeMetricEventCount = "dialcode-metric-event-count"
  val successDialcodeMetricEventCount = "dialcode-metric-event-success-count"
  val failedDialcodeMetricEventCount = "dialcode-metric-event-failed-count"

  // DIAL Code Indexer Metric List
  val dialcodeExternalEventCount = "dialcode-external-event-count"
  val successDialcodeExternalEventCount =
    "dialcode-external-event-success-count"
  val failedDialcodeExternalEventCount = "dialcode-external-event-failed-count"

  // Audit Event Generator Metric List
  val totalAuditEventsCount = "total-audit-events-count"
  val failedAuditEventsCount = "failed-audit-events-count"
  val auditEventSuccessCount = "audit-event-success-count"

  // Audit Event Generator Metric List
  val totalAuditHistoryEventsCount = "total-audit-history-events-count"
  val failedAuditHistoryEventsCount = "failed-audit-history-events-count"
  val auditHistoryEventSuccessCount = "audit-history-event-success-count"

  // Obsrv MetaData Generator Metric List
  val totalObsrvMetaDataGeneratorEventsCount =
    "total-obsrv-metadata-events-count"
  val failedObsrvMetaDataGeneratorEventsCount =
    "failed-obsrv-metadata-events-count"
  val obsrvMetaDataGeneratorEventsSuccessCount =
    "audit-obsrv-metadata-success-count"

  // Consumers
  val transactionEventConsumer = "transaction-event-processor-consumer"
  val auditEventGeneratorFunction = "audit-event-generator-function"
  val auditHistoryIndexerFunction = "audit-history-indexer-function"
  val obsrvMetaDataGeneratorFunction = "obsrv-metadata-generator-function"
  val transactionEventRouterFunction = "transaction-event-router-function"
  val auditEventProducer = "audit-event-generator-producer"
  val obsrvEventProducer = "obsrv-metadata-generator-producer"
  val searchIndexerConsumer = "search-indexer-consumer"
  val transactionEventRouter = "transaction-event-router"

  val basePath = config.getString("schema.basePath")
  val configVersion = "1.0"

  // ES Configs
  val esConnectionInfo = config.getString("es.basePath")

  val timeZone =
    if (config.hasPath("timezone")) config.getString("timezone") else "IST"
  val auditHistoryIndex = "kp_audit_log"
  val auditHistoryIndexType = "ah"

  val compositeSearchIndex: String =
    if (config.hasPath("compositesearch.index.name"))
      config.getString("compositesearch.index.name")
    else "compositesearch"
  val compositeSearchIndexType: String = "cs"

  val dialcodeExternalIndex: String =
    if (config.hasPath("dialcode.index.name"))
      config.getString("dialcode.index.name")
    else "dialcode"
  val dialcodeExternalIndexType: String = "dc"

  val dialcodeMetricIndex: String =
    if (config.hasPath("dailcodemetrics.index.name"))
      config.getString("dailcodemetrics.index.name")
    else "dialcodemetrics"
  val dialcodeMetricIndexType: String = "dcm"

  val restrictMetadataObjectTypes: util.List[String] =
    if (config.hasPath("restrict.metadata.objectTypes"))
      config.getStringList("restrict.metadata.objectTypes")
    else new util.ArrayList[String]
  val nestedFields: util.List[String] =
    if (config.hasPath("nested.fields")) config.getStringList("nested.fields")
    else new util.ArrayList[String]
  val definitionBasePath: String =
    if (config.hasPath("schema.basePath")) config.getString("schema.basePath")
    else
      "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap =
    if (config.hasPath("schema.supportedVersion"))
      config.getObject("schema.supportedVersion").unwrapped().asScala
    else Map[String, AnyRef]()
  val definitionCacheExpiry: Int =
    if (config.hasPath("schema.definition_cache.expiry"))
      config.getInt("schema.definition_cache.expiry")
    else 14400
  val restrictObjectTypes: util.List[String] =
    if (config.hasPath("restrict.objectTypes"))
      config.getStringList("restrict.objectTypes")
    else new util.ArrayList[String]
  val ignoredFields: List[String] =
    if (config.hasPath("ignored.fields"))
      config.getStringList("ignored.fields").asScala.toList
    else List("responseDeclaration", "body")

  val isrRelativePathEnabled: Boolean =
    if (config.hasPath("cloudstorage.metadata.replace_absolute_path"))
      config.getBoolean("cloudstorage.metadata.replace_absolute_path")
    else false
  val esImage: String =
    if (config.hasPath("es.image")) config.getString("es.image")
    else "docker.elastic.co/elasticsearch/elasticsearch"
  val esImageTag: String =
    if (config.hasPath("es.imageTag")) config.getString("es.imageTag")
    else "7.10.2"
  val esPorts: util.List[String] =
    if (config.hasPath("es.ports")) config.getStringList("es.ports")
    else List("9200:9200").asJava
  val esJavaOpts: String =
    if (config.hasPath("es.javaOpts")) config.getString("es.javaOpts")
    else "-Xms128m -Xmx512m"
  val esJavaOptsKey: String =
    if (config.hasPath("es.javaOptsKey")) config.getString("es.javaOptsKey")
    else "ES_JAVA_OPTS"
  val xpackSecurityEnabled: String =
    if (config.hasPath("es.xpackSecurityEnabled"))
      config.getString("es.xpackSecurityEnabled")
    else "false"
  val xpackSecurityKey: String =
    if (config.hasPath("es.xpackSecurityKey"))
      config.getString("es.xpackSecurityKey")
    else "xpack.security.enabled"
}
