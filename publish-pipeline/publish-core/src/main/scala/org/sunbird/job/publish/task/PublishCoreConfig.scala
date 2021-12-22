package org.sunbird.job.publish.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.PublishMetadata

import java.util

class PublishCoreConfig(override val config: Config) extends PublishConfig(config, "chain-publish"){

	implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
	implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
	implicit val publishMetaTypeInfo: TypeInformation[PublishMetadata] = TypeExtractor.getForClass(classOf[PublishMetadata])

	// Job Configuration
	val jobEnv: String = config.getString("job.env")

	// Kafka Topics Configuration
	val kafkaInputTopic: String = config.getString("kafka.input.topic")
	val postPublishTopic: String = config.getString("kafka.post_publish.topic")
	val inputConsumerName = "publish-chain-consumer"
	val questionSetTopic : String = config.getString("kafka.event.questionset.topic")
	val contentTopic : String = config.getString("kafka.event.content.topic")

	// Parallelism
	override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
	val eventRouterParallelism: Int = config.getInt("task.router.parallelism")

	// Metric List
	val totalEventsCount = "total-events-count"
	val skippedEventCount = "skipped-event-count"
	val publishChainEventCount = "publishchain-count"
	val publishChainSuccessEventCount = "publishchain-success-count"
	val publishChainFailedEventCount = "publishchain-failed-count"

	// Cassandra Configurations
	val cassandraHost: String = config.getString("lms-cassandra.host")
	val cassandraPort: Int = config.getInt("lms-cassandra.port")

	// Out Tags
	val publishChainOutTag: OutputTag[PublishMetadata] = OutputTag[PublishMetadata]("chain-publish")




}
