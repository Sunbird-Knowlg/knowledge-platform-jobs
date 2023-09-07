package org.sunbird.job.transaction.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

class TransactionEventProcessorConfig(override val config: Config) extends BaseJobConfig(config, "transaction-event-processor") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaOutputTopic: String = config.getString("kafka.output.audit.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")
  val kafkaProducerParallelism: Int = config.getInt("task.producer.parallelism")
  val auditEventGenerator: Boolean = config.getBoolean("job.audit-event-generator")
  val auditHistoryIndexer: Boolean = config.getBoolean("job.audit-history-indexer")
  val obsrvMetadataGenerator: Boolean = config.getBoolean("job.obsrv-metadata-generator")
  val kafkaObsrvOutputTopic: String = config.getString("kafka.output.obsrv.topic")

  val auditOutputTag: OutputTag[String] = OutputTag[String]("audit-event-tag")
  val obsrvAuditOutputTag: OutputTag[String] = OutputTag[String]("obsrv-metadata-tag")

  val defaultChannel: String =config.getString("channel.default")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-events-count"
  val emptySchemaEventCount = "empty-schema-events-count"
  val emptyPropsEventCount = "empty-props-events-count"
  val esFailedEventCount = "elasticsearch-error-events-count"

  // Consumers
  val transactionEventConsumer = "transaction-event-processor-consumer"
  val auditEventGeneratorFunction = "audit-event-generator-function"
  val auditHistoryIndexerFunction = "audit-history-indexer-function"
  val obsrvMetaDataGeneratorFunction = "obsrv-metadata-generator-function"
  val transactionEventProducer = "transaction-event-processor-producer"


  val basePath = config.getString("schema.basePath")
  val configVersion = "1.0"

  // ES Configs
  val esConnectionInfo = config.getString("es.basePath")

  val timeZone = if (config.hasPath("timezone")) config.getString("timezone") else "IST"
  val auditHistoryIndex = "kp_audit_log"
  val operationCreate = "CREATE"
  val auditHistoryIndexType = "ah"
}
