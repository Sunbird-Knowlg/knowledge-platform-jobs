package org.sunbird.job.content.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.content.publish.domain.Event
import org.sunbird.job.publish.config.PublishConfig

import java.util

class InteractiveContentPublishConfig(override val config: Config) extends PublishConfig(config, "content-publish") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val publishMetaTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Job Configuration
  val jobEnv: String = config.getString("job.env")

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val postPublishTopic: String = config.getString("kafka.post_publish.topic")
  val kafkaErrorTopic: String = config.getString("kafka.error.topic")
  val inputConsumerName = "content-publish-consumer"

  // Parallelism
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val eventRouterParallelism: Int = config.getInt("task.router.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val skippedEventCount = "skipped-event-count"
  val publishChainEventCount = "publish-chain-event-count"
  val publishChainSuccessEventCount = "publish-chain-event-success-count"
  val publishChainFailedEventCount = "publish-chain-event-failed-count"


  // Redis Configurations
  val nodeStore: Int = config.getInt("redis.database.contentCache.id")

  // Out Tags
  val publishChainEventOutTag: OutputTag[Event] = OutputTag[Event]("event-publish")
  val failedEventOutTag: OutputTag[String] = OutputTag[String]("failed-event")

  // Publish Chain Event Topics
  val questionSetTopic : String = config.getString("kafka.questionset_publish.topic")
  val contentPublishTopic : String = config.getString("kafka.content_publish.topic")

}
