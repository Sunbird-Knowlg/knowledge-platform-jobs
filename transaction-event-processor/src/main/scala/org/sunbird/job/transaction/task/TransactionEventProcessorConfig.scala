package org.sunbird.job.transaction.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.transaction.domain.Event

class TransactionEventProcessorConfig(override val config: Config) extends BaseJobConfig(config, "transaction-event-processor") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaAuditOutputTopic: String = config.getString("kafka.output.audit.topic")
  val kafkaObsrvOutputTopic: String = config.getString("kafka.output.obsrv.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")

  //Flink Jobs Configuration
  val kafkaProducerParallelism: Int = config.getInt("task.producer.parallelism")
  val auditEventGenerator: Boolean = config.getBoolean("job.audit-event-generator")
  val auditHistoryIndexer: Boolean = config.getBoolean("job.audit-history-indexer")
  val obsrvMetadataGenerator: Boolean = config.getBoolean("job.obsrv-metadata-generator")

  val outputTag: OutputTag[Event] = OutputTag[Event]("output-tag")
  val auditOutputTag: OutputTag[String] = OutputTag[String]("audit-event-tag")
  val obsrvAuditOutputTag: OutputTag[String] = OutputTag[String]("obsrv-metadata-tag")

  val defaultChannel: String = config.getString("channel.default")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-events-count"
  val emptySchemaEventCount = "empty-schema-events-count"
  val emptyPropsEventCount = "empty-props-events-count"
  val esFailedEventCount = "elasticsearch-error-events-count"

  //Audit Event Generator Metric List
  val totalAuditEventsCount = "total-audit-events-count"
  val failedAuditEventsCount = "failed-audit-events-count"
  val auditEventSuccessCount = "audit-event-success-count"

  //Audit Event Generator Metric List
  val totalAuditHistoryEventsCount = "total-audit-history-events-count"
  val failedAuditHistoryEventsCount = "failed-audit-history-events-count"
  val auditHistoryEventSuccessCount = "audit-history-event-success-count"

  //Obsrv MetaData Generator Metric List
  val totalObsrvMetaDataGeneratorEventsCount = "total-obsrv-metadata-events-count"
  val failedObsrvMetaDataGeneratorEventsCount = "failed-obsrv-metadata-events-count"
  val obsrvMetaDataGeneratorEventsSuccessCount = "audit-obsrv-metadata-success-count"


  // Consumers
  val transactionEventConsumer = "transaction-event-processor-consumer"
  val auditEventGeneratorFunction = "audit-event-generator-function"
  val auditHistoryIndexerFunction = "audit-history-indexer-function"
  val obsrvMetaDataGeneratorFunction = "obsrv-metadata-generator-function"
  val transactionEventRouterFunction = "transaction-event-router-function"
  val auditEventProducer = "audit-event-generator-producer"
  val obsrvEventProducer = "obsrv-metadata-generator-producer"


  val basePath = config.getString("schema.basePath")
  val configVersion = "1.0"

  // ES Configs
  val esConnectionInfo = config.getString("es.basePath")

  val timeZone = if (config.hasPath("timezone")) config.getString("timezone") else "IST"
  val auditHistoryIndex = "kp_audit_log"
  val auditHistoryIndexType = "ah"
}
