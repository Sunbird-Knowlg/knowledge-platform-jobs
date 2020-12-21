package org.sunbird.job.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.functions.PublishMetadata

class PostPublishProcessorConfig(override val config: Config) extends BaseJobConfig(config, "post-publish-processor")  {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val publishMetaTypeInfo: TypeInformation[PublishMetadata] = TypeExtractor.getForClass(classOf[PublishMetadata])

  // Job Configuration
  val jobEnv: String = config.getString("job.env")

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val contentPublishTopic: String = config.getString("kafka.publish.topic")

  val inputConsumerName = "post-publish-event-consumer"

  // Parallelism
  val eventRouterParallelism: Int = config.getInt("task.router.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"

  // Cassandra Configurations
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val lmsKeyspaceName = config.getString("lms-cassandra.keyspace")
  val batchTableName = config.getString("lms-cassandra.batchTable")

  // Neo4J Configurations
  val graphRoutePath = config.getString("neo4j.routePath")
  val graphName = config.getString("neo4j.graph")


  // Tags
  val batchCreateOutTag: OutputTag[java.util.Map[String, AnyRef]] = OutputTag[java.util.Map[String, AnyRef]]("batch-create")
  val linkDIALCodeOutTag: OutputTag[java.util.Map[String, AnyRef]] = OutputTag[java.util.Map[String, AnyRef]]("dialcode-link")
  val shallowContentPublishOutTag: OutputTag[PublishMetadata] = OutputTag[PublishMetadata]("shallow-copied-content-publish")
  val publishEventOutTag: OutputTag[String] = OutputTag[String]("content-publish-request")


  val searchBaseUrl = config.getString("content.search.basePath")
  val lmsBaseUrl = config.getString("lms.basePath")


}
