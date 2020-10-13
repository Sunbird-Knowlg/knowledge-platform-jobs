package org.sunbird.job.task

import java.util

import com.typesafe.config.Config
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

class CertificateGeneratorConfig(override val config: Config) extends BaseJobConfig(config, "certificate-generator") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")


  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"
  val cacheWrite = "cache-write-count"
  val dbReadCount = "db-read-count"

  // Consumers
  val certificateGeneratorConsumer = "certificate"

  // env vars
  val storageType: String = config.getString("CLOUD_STORAGE_TYPE")
  val containerName: String = config.getString("CONTAINER_NAME")
  val azureStorageSecret: String = config.getString("AZURE_STORAGE_SECRET")
  val azureStorageKey: String = config.getString("AZURE_STORAGE_KEY")
  val awsStorageSecret: String = ""
  val awsStorageKey: String = ""
  val domainUrl: String = config.getString("sunbird_cert_domain_url")
  val encServiceUrl: String = config.getString("sunbird_cert_enc_service_url")
  val slug: String = config.getString("sunbird_cert_slug")
  val basePath: String = domainUrl.concat("/").concat(if (StringUtils.isNotBlank(slug)) slug else "certs")
  val certRegistryBaseUrl = "http://localhost:9000"

  //tags
  val addCertificateOutTag: OutputTag[java.util.Map[String, AnyRef]] = OutputTag[java.util.Map[String, AnyRef]]("add-certificate")


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
}
