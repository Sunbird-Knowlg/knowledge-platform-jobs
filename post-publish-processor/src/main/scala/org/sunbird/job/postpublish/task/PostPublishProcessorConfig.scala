package org.sunbird.job.postpublish.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.postpublish.functions.PublishMetadata

import java.util

class PostPublishProcessorConfig(override val config: Config) extends BaseJobConfig(config, "post-publish-processor") {

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
  val shallowCopyParallelism: Int = config.getInt("task.shallow_copy.parallelism")
  val linkDialCodeParallelism: Int = config.getInt("task.link_dialcode.parallelism")
  val batchCreateParallelism: Int = config.getInt("task.batch_create.parallelism")
  val dialcodeContextUpdaterParallelism: Int = config.getInt("task.dialcode_context_updater.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val skippedEventCount = "skipped-events-count"
  val batchCreationCount = "batch-creation-count"
  val batchCreationSuccessCount = "batch-creation-success-count"
  val batchCreationFailedCount = "batch-creation-failed-count"
  val shallowCopyCount = "shallow-copy-count"
  val dialLinkCount = "dial-link-count"
  val dialLinkSuccessCount = "dial-link-success-count"
  val dialLinkFailedCount = "dial-link-failed-count"
  val qrImageGeneratorEventCount = "qr-image-event-count"
  val dialcodeContextUpdaterCount = "dial-context-count"
  val dialcodeContextUpdaterSuccessCount = "dial-context-success-count"
  val dialcodeContextUpdaterFailedCount = "dial-context-failed-count"

  // Cassandra Configurations
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val lmsKeyspaceName: String = config.getString("lms-cassandra.keyspace")
  val batchTableName: String = config.getString("lms-cassandra.batchTable")
  val dialcodeKeyspaceName: String = config.getString("dialcode-cassandra.keyspace")
  val dialcodeTableName: String = config.getString("dialcode-cassandra.imageTable")

  // Neo4J Configurations
  val graphRoutePath: String = config.getString("neo4j.routePath")
  val graphName: String = config.getString("neo4j.graph")


  // Tags
  val batchCreateOutTag: OutputTag[java.util.Map[String, AnyRef]] = OutputTag[java.util.Map[String, AnyRef]]("batch-create")
  val linkDIALCodeOutTag: OutputTag[java.util.Map[String, AnyRef]] = OutputTag[java.util.Map[String, AnyRef]]("dialcode-link")
  val dialcodeContextOutTag: OutputTag[java.util.Map[String, AnyRef]] = OutputTag[java.util.Map[String, AnyRef]]("dialcode-context")
  val shallowContentPublishOutTag: OutputTag[PublishMetadata] = OutputTag[PublishMetadata]("shallow-copied-content-publish")
  val publishEventOutTag: OutputTag[String] = OutputTag[String]("content-publish-request")
  val generateQRImageOutTag: OutputTag[String] = OutputTag[String]("qr-image-generator-request")
  val dialcodeContextUpdaterOutTag: OutputTag[String] = OutputTag[String]("dialcode-context-updater")

  val searchBaseUrl: String = config.getString("service.search.basePath")
  val lmsBaseUrl: String = config.getString("service.lms.basePath")
  val learningBaseUrl: String = config.getString("service.learning_service.basePath")
  val dialBaseUrl: String = config.getString("service.dial.basePath")

  // API URLs
  val batchCreateAPIPath: String = lmsBaseUrl + "/private/v1/course/batch/create"
  val searchAPIPath: String = searchBaseUrl + "/v3/search"
  val reserveDialCodeAPIPath: String = learningBaseUrl + "/content/v3/dialcode/reserve"

  // QR Image Generator
  val QRImageGeneratorTopic: String = config.getString("kafka.qrimage.topic")
  val primaryCategories: util.List[String] = if (config.hasPath("dialcode.linkable.primaryCategory")) config.getStringList("dialcode.linkable.primaryCategory") else util.Arrays.asList("Course") //List[String]("Course")
  val dialcodeContextUpdaterTopic: String = config.getString("kafka.dialcode.context.topic")

}
