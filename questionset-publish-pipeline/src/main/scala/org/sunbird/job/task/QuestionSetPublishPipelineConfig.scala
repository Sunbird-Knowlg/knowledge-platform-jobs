package org.sunbird.job.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.publish.domain.PublishMetadata
import org.sunbird.publish.config.PublishConfig

class QuestionSetPublishPipelineConfig(override val config: Config) extends PublishConfig(config, "questionset-publish-pipeline"){

	implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
	implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
	implicit val publishMetaTypeInfo: TypeInformation[PublishMetadata] = TypeExtractor.getForClass(classOf[PublishMetadata])

	// Job Configuration
	val jobEnv: String = config.getString("job.env")

	// Kafka Topics Configuration
	val kafkaInputTopic: String = config.getString("kafka.input.topic")
	val postPublishTopic: String = config.getString("kafka.post_publish.topic")
	val inputConsumerName = "questionset-publish-pipeline-consumer"

	// Parallelism
	override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
	val eventRouterParallelism: Int = config.getInt("task.router.parallelism")

	// Metric List
	val totalEventsCount = "total-events-count"
	val successEventCount = "success-events-count"
	val failedEventCount = "failed-events-count"
	val skippedEventCount = "skipped-event-count"

	// Cassandra Configurations
	val cassandraHost: String = config.getString("kp-cassandra.host")
	val cassandraPort: Int = config.getInt("kp-cassandra.port")
	val questionSetKeyspaceName = config.getString("questionset.hierarchy_keyspace")
	val questionSetTableName = config.getString("questionset.table")

	// Neo4J Configurations
	val graphRoutePath = config.getString("neo4j.routePath")
	val graphName = config.getString("neo4j.graph")
}
