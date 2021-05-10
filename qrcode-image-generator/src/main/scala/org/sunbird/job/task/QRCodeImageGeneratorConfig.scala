package org.sunbird.job.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.BaseJobConfig

class QRCodeImageGeneratorConfig(override val config: Config) extends BaseJobConfig(config, "qrcode-image-generator") {

  private val serialVersionUID = 2905979434303791378L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val dbHitEventCount = "db-hit-events-count"
  val dbFailureEventCount = "db-failure-events-count"
  val skippedEventCount = "skipped-events-count"

  // Consumers
  val eventConsumer = "qrcode-image-generator-consumer"
  val qrCodeImageGeneratorFunction = "qrcode-image-generator-function"

  val configVersion = "1.0"

  val eid = "BE_QR_IMAGE_GENERATOR"
  val lpTempfileLocation = "/tmp"

  //Cloud store details
  val cloudStorageType= config.getString("cloud.storage.type")
  val cloudUploadRetryCount= config.getInt("cloud.upload.retry.count")
  val azureStorageKey= config.getString("azure.storage.key")
  val azureStorageSecret= config.getString("azure.storage.secret")
  val awsStorageKey= config.getString("aws.storage.key")
  val awsStorageSecret= config.getString("aws.storage.secret")

  //image Margings
  val qrImageBottomMargin= config.getInt("qr.image.bottom.margin")
  val qrImageMargin= config.getInt("qr.image.margin")

  //cassandra config
  val cassandraHost= config.getString("lms-cassandra.host")
  val cassandraPort= config.getInt("lms-cassandra.port")
}
