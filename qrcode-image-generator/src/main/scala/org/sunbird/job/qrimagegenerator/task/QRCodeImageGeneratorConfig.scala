package org.sunbird.job.qrimagegenerator.task

import com.typesafe.config.Config
import org.sunbird.job.BaseJobConfig

class QRCodeImageGeneratorConfig(override val config: Config) extends BaseJobConfig(config, "qrcode-image-generator") {

  // Job Configuration
  val jobEnv: String = config.getString("job.env")

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val dbHitEventCount = "db-hit-events-count"
  val dbFailureEventCount = "db-failure-events-count"
  val skippedEventCount = "skipped-events-count"
  val cloudDbHitCount = "cloud-db-hit-events-count"
  val cloudDbFailCount = "cloud-db-hit-failure-count"

  // Consumers
  val eventConsumer = "qrcode-image-generator-consumer"
  val qrCodeImageGeneratorFunction = "qrcode-image-generator-function"

  val configVersion = "1.0"

  val eid = "BE_QR_IMAGE_GENERATOR"
  val lpTempFileLocation: String = config.getString("lp.tmp.file.location")

  //image Margins
  val qrImageBottomMargin: Int = config.getInt("qr.image.bottom.margin")
  val qrImageMargin: Int = config.getInt("qr.image.margin")

  //cassandra config
  val cassandraHost: String = config.getString("lms-cassandra.host")
  val cassandraPort: Int = config.getInt("lms-cassandra.port")
  val cassandraKeyspace: String = config.getString("lms-cassandra.keyspace")
  val cassandraDialCodeImageTable: String = config.getString("lms-cassandra.dialcode.imageTable")
  val cassandraDialCodeBatchTable: String = config.getString("lms-cassandra.dialcode.batchTable")
}
