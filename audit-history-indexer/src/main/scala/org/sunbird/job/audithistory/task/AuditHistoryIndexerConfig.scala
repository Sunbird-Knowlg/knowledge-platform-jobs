package org.sunbird.job.audithistory.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.BaseJobConfig

class AuditHistoryIndexerConfig(override val config: Config) extends BaseJobConfig(config, "audit-history-indexer") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val esFailedEventCount = "elasticsearch-error-events-count"
  val skippedEventCount = "skipped-events-count"

  // Consumers
  val eventConsumer = "audit-history-indexer-consumer"
  val auditHistoryIndexerFunction = "audit-history-indexer-function"

  val configVersion = "1.0"

  // ES Configs
  val esConnectionInfo = config.getString("es.basePath")

  val timeZone =  if(config.hasPath("timezone")) config.getString("timezone") else "IST"
  val auditHistoryIndex = "kp_audit_log"
  val operationCreate = "CREATE"
  val auditHistoryIndexType = "ah"
}
