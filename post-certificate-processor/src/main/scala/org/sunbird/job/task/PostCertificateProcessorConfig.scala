package org.sunbird.job.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

class PostCertificateProcessorConfig(override val config: Config) extends BaseJobConfig(config, "post-certificate-process") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaFailedEventTopic: String = config.getString("kafka.output.failed.topic")
  val kafkaAuditEventTopic: String = config.getString("kafka.output.audit.topic")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"
  val dbReadCount = "db-read-count"
  val dbUpdateCount = "db-update-user-enrollment-count"
  val notifiedUserCount = "notified-user-count"
  val skipNotifyUserCount = "skipped-notify-user-count"


  // Consumers
  val postCertificateProcessConsumer = "post-certificate-process-consumer"

  // Producers
  val postCertificateProcessFailedEventProducer = "post-certificate-process-failed-events-sink"
  val postCertificateProcessAuditProducer = "post-certificate-process-audit-events-sink"


  // Cassandra Configurations
  val dbEnrollmentTable: String = config.getString("lms-cassandra.enrollment.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")

  val learnerServiceBaseUrl: String = config.getString("learner-service.basePath")
  val dbCourseBatchTable: String = config.getString("lms-cassandra.course_batch.table")
  val notificationEndPoint: String = "/v2/notification"

  // tags
  val auditEventOutputTagName = "audit-events"
  val auditEventOutputTag: OutputTag[String] = OutputTag[String](auditEventOutputTagName)
  val failedEventOutputTagName = "failed-events"
  val failedEventOutputTag: OutputTag[String] = OutputTag[String](failedEventOutputTagName)

  // constants

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


}
