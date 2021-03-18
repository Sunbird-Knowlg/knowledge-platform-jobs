package org.sunbird.job.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

class AutoCreatorV2Config(override val config: Config) extends BaseJobConfig(config, "auto-creator-v2") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaOutputTopic: String = config.getString("kafka.output.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")
  val kafkaProducerParallelism: Int = config.getInt("task.producer.parallelism")

  val autoCreatorOutputTag: OutputTag[String] = OutputTag[String]("auto-creator-event-tag")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-events-count"

  // Consumers
  val eventConsumer = "auto-creator-v2-consumer"
  val autoCreatorV2Function = "auto-creator-v2-function"
  val autoCreatorEventProducer = "auto-creator-v2-producer"

  val configVersion = "1.0"

  // Redis Configurations
  val relationCacheStore: Int = config.getInt("redis.database.relationCache.id")
  val collectionCacheStore: Int = config.getInt("redis.database.collectionCache.id")
}
