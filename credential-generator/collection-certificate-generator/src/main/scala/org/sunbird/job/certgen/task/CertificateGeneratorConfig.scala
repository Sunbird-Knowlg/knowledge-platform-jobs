package org.sunbird.job.certgen.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.certgen.functions.{NotificationMetaData, UserFeedMetaData}

class CertificateGeneratorConfig(override val config: Config) extends BaseJobConfig(config, "collection-certificate-generator") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val notificationMetaTypeInfo: TypeInformation[NotificationMetaData] = TypeExtractor.getForClass(classOf[NotificationMetaData])
  implicit val userFeeMetaTypeInfo: TypeInformation[UserFeedMetaData] = TypeExtractor.getForClass(classOf[UserFeedMetaData])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaAuditEventTopic: String = config.getString("kafka.output.audit.topic")


  // Producers
  val certificateGeneratorAuditProducer = "collection-certificate-generator-audit-events-sink"

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val notifierParallelism: Int = if(config.hasPath("task.notifier.parallelism")) config.getInt("task.notifier.parallelism") else 1
  val userFeedParallelism: Int = if(config.hasPath("task.userfeed.parallelism")) config.getInt("task.userfeed.parallelism") else 1



  // Cassandra Configurations
  val dbEnrollmentTable: String = config.getString("lms-cassandra.user_enrolments.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val dbCourseBatchTable: String = config.getString("lms-cassandra.course_batch.table")
  val dbBatchId = "batchid"
  val dbCourseId = "courseid"
  val dbUserId = "userid"
  val active: String = "active"
  val issuedCertificates: String = "issued_certificates"

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"
  val enrollmentDbReadCount = "enrollment-db-read-count"
  val dbUpdateCount = "db-update-user-enrollment-count"
  val notifiedUserCount = "notified-user-count"
  val skipNotifyUserCount = "skipped-notify-user-count"
  val courseBatchdbReadCount = "db-course-batch-read-count"

  // Consumers
  val certificateGeneratorConsumer = "certificate"

  // env vars
  val storageType: String = config.getString("cert_cloud_storage_type")
  val containerName: String = config.getString("cert_container_name")
  val azureStorageSecret: String = config.getString("cert_azure_storage_secret")
  val azureStorageKey: String = config.getString("cert_azure_storage_key")
  val domainUrl: String = config.getString("cert_domain_url")
  val encServiceUrl: String = config.getString("service.enc.basePath")
  val certRegistryBaseUrl: String = config.getString("service.certreg.basePath")
  val learnerServiceBaseUrl: String = config.getString("service.learner.basePath")
  val notificationServiceBaseUrl: String = config.getString("service.notification.basePath")
  val basePath: String = domainUrl.concat("/").concat("certs")
  val awsStorageSecret: String = ""
  val awsStorageKey: String = ""
  val addCertRegApi = "/certs/v2/registry/add"
  val userFeedCreateEndPoint:String = "/v2/notification/send"
  val notificationEndPoint: String = "/v2/notification"

  //constant
  val DATA: String = "data"
  val RECIPIENT_NAME: String = "recipientName"
  val ISSUER: String = "issuer"
  val BADGE_URL: String = "/Badge.json"
  val ISSUER_URL: String = basePath.concat("/Issuer.json")
  val EVIDENCE_URL: String = basePath.concat("/Evidence.json")
  val CONTEXT: String = basePath.concat( "/v1/context.json")
  val PUBLIC_KEY_URL: String = "_publicKey.json"
  val VERIFICATION_TYPE: String = "SignedBadge"
  val SIGNATORY_EXTENSION: String = basePath.concat("v1/extensions/SignatoryExtension/context.json")
  val ACCESS_CODE_LENGTH: String = "6"
  val EDATA: String = "edata"
  val RELATED: String = "related"
  val OLD_ID: String = "oldId"
  val BATCH_ID: String = "batchId"
  val COURSE_ID: String = "courseId"
  val TEMPLATE_ID: String = "templateId"
  val USER_ID: String = "userId"


  val courseId = "courseId"
  val batchId = "batchId"
  val userId = "userId"
  val notifyTemplate = "notifyTemplate"
  val firstName = "firstName"
  val trainingName = "TrainingName"
  val heldDate = "heldDate"
  val recipientUserIds = "recipientUserIds"
  val identifier = "identifier"
  val body = "body"
  val notificationSmsBody = "Congratulations! Download your course certificate from your profile page. If you have a problem downloading it on the mobile, update your DIKSHA app"
  val request = "request"
  val filters = "filters"
  val fields = "fields"
  val issued_certificates = "issued_certificates"
  val eData = "edata"
  val name = "name"
  val token = "token"
  val lastIssuedOn = "lastIssuedOn"
  val certificate = "certificate"
  val action = "action"
  val courseName = "courseName"
  val templateId = "templateId"
  val cert_templates = "cert_templates"
  val courseBatch = "CourseBatch"
  val l1 = "l1"
  val id = "id"
  val data = "data"
  val category = "category"
  val certificates = "certificates"


  // Tags
  val auditEventOutputTagName = "audit-events"
  val auditEventOutputTag: OutputTag[String] = OutputTag[String](auditEventOutputTagName)
  val notifierOutputTag: OutputTag[NotificationMetaData] = OutputTag[NotificationMetaData]("notifier")
  val userFeedOutputTag: OutputTag[UserFeedMetaData] = OutputTag[UserFeedMetaData]("user-feed")
  
  //UserFeed constants
  val priority: String = "priority"
  val userFeedMsg: String = "You have earned a certificate! Download it from your profile page."
  val priorityValue = 1
  val userFeedCount = "user-feed-count"
  
}
