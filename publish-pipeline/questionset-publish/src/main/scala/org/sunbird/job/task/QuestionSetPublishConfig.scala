package org.sunbird.job.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.publish.domain.PublishMetadata
import org.sunbird.job.BaseJobConfig

class QuestionSetPublishConfig(override val config: Config) extends BaseJobConfig(config, "questionset-publish"){

	implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
	implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
	implicit val publishMetaTypeInfo: TypeInformation[PublishMetadata] = TypeExtractor.getForClass(classOf[PublishMetadata])

	// Job Configuration
	val jobEnv: String = config.getString("job.env")

	// Kafka Topics Configuration
	val kafkaInputTopic: String = config.getString("kafka.input.topic")
	val postPublishTopic: String = config.getString("kafka.post_publish.topic")
	val inputConsumerName = "questionset-publish-consumer"

	// Parallelism
	override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
	val eventRouterParallelism: Int = config.getInt("task.router.parallelism")

	// Metric List
	val totalEventsCount = "total-events-count"
	val successEventCount = "success-events-count"
	val failedEventCount = "failed-events-count"
	val skippedEventCount = "skipped-event-count"
	val questionPublishEventCount = "question-publish-count"
	val questionSetPublishEventCount = "questionset-publish-count"
	val dbUpdateCount = "db-update-count"
	val dbReadCount = "db-read-count"
	val cacheHitCount = "cache-hit-count"
	val cacheMissCount = "cache-miss-count"

	// Cassandra Configurations
	val cassandraHost: String = config.getString("lms-cassandra.host")
	val cassandraPort: Int = config.getInt("lms-cassandra.port")
	val questionKeyspaceName = config.getString("question.keyspace")
	val questionTableName = config.getString("question.table")
	val questionSetKeyspaceName = config.getString("questionset.keyspace")
	val questionSetTableName = config.getString("questionset.table")

	// Neo4J Configurations
	val graphRoutePath = config.getString("neo4j.routePath")
	val graphName = config.getString("neo4j.graph")

	// Out Tags
	val questionPublishOutTag: OutputTag[PublishMetadata] = OutputTag[PublishMetadata]("question-publish")
	val questionSetPublishOutTag: OutputTag[PublishMetadata] = OutputTag[PublishMetadata]("questionset-publish")
}
