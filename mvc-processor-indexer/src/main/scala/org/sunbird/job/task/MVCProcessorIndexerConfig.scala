package org.sunbird.job.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

class MVCProcessorIndexerConfig(override val config: Config) extends BaseJobConfig(config, "mvc-processor-indexer") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")
  val kafkaFailedTopic: String = config.getString("output.failed.topic")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val runtimeFailedEventCount = "runtime-failed-events-count"
  val esFailedEventCount = "elasticsearch-error-events-count"
  val skippedEventCount = "skipped-events-count"
  val csFailedEventCount = "cassandra-failed-events-count"
  val contentApiFailedEventCount = "content-api-failed-events-count"

  // Consumers
  val eventConsumer = "mvc-processor-indexer-consumer"
  val mvcProcessorIndexerFunction = "mvc-processor-indexer-function"
  val mvcFailedEventProducer = "mvc-processor-indexer-producer"

  val failedOutputTag: OutputTag[String] = OutputTag[String]("failed-event-tag")

  val configVersion = "1.0"

  // ES Configs
  val esConnectionInfo = config.getString("es.basePath")
  val nestedFields = List(config.getString("nested.fields").split(","))
  val mvcProcessorIndex = "mvc-content-v1"
  val mvcProcessorIndexType = "_doc"
  val operationCreate = "CREATE"

  val contentServiceBase = config.getString("")
  val mlVectorAPI = config.getString("ml.vector.api")
  val mlKeywordAPI = config.getString("ml.keyword.api")

  val timeZone =  if(config.hasPath("timezone")) config.getString("timezone") else "IST"
}
