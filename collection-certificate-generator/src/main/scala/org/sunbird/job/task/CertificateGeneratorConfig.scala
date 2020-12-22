package org.sunbird.job.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

class CertificateGeneratorConfig(override val config: Config) extends BaseJobConfig(config, "collection-certificate-generator") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaFailedEventTopic: String = config.getString("kafka.output.failed.topic")


  // Producers
  val certificateGeneratorFailedEventProducer = "certificate-generate-failed-sink"

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")


  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"

  // Consumers
  val certificateGeneratorConsumer = "certificate"

  // env vars
  val storageType: String = config.getString("cert_cloud_storage_type")
  val containerName: String = config.getString("cert_container_name")
  val azureStorageSecret: String = config.getString("cert_azure_storage_secret")
  val azureStorageKey: String = config.getString("cert_azure_storage_key")
  val domainUrl: String = config.getString("cert_domain_url")
  val encServiceUrl: String = config.getString("enc-service.basePath")
  val certRegistryBaseUrl: String = config.getString("cert-reg.basePath")
  val basePath: String = domainUrl.concat("/").concat("certs")
  val awsStorageSecret: String = ""
  val awsStorageKey: String = ""

  //constant
  val DATA: String = "data"
  val RECIPIENT_NAME: String = "recipientName"
  val ISSUER: String = "issuer"
  val BADGE_URL: String = "/Badge.json"
  val ISSUER_URL: String = "/Issuer.json"
  val EVIDENCE_URL: String = "/Evidence.json"
  val CONTEXT: String = "/v1/context.json"
  val PUBLIC_KEY_URL: String = "_publicKey.json"
  val VERIFICATION_TYPE: String = "SignedBadge"
  val SIGNATORY_EXTENSION: String = "v1/extensions/SignatoryExtension"
  val ACCESS_CODE_LENGTH: String = "6"
  val EDATA: String = "edata"
  val RELATED: String = "related"
  val OLD_ID: String = "oldId"
  val BATCH_ID: String = "batchId"
  val COURSE_ID: String = "courseId"
  val TEMPLATE_ID: String = "templateId"
  val USER_ID: String = "userId"

  // Tags

  val failedEventOutputTagName = "failed-events"
  val failedEventOutputTag: OutputTag[String] = OutputTag[String](failedEventOutputTagName)
  val notifierOutputTag: OutputTag[java.util.Map[String, AnyRef]] = OutputTag[java.util.Map[String, AnyRef]]("notifier")
  val userFeedOutputTag: OutputTag[java.util.Map[String, AnyRef]] = OutputTag[java.util.Map[String, AnyRef]]("user-feed")

}
