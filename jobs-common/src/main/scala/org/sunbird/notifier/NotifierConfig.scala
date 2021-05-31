package org.sunbird.notifier

import com.typesafe.config.Config
import org.sunbird.job.BaseJobConfig

class NotifierConfig(override val config: Config) extends BaseJobConfig(config, "notifier") {


  private val serialVersionUID = 2905979434303791379L

  val learnerServiceBaseUrl: String = config.getString("service.learner.basePath")
  val notificationEndPoint: String = "/v2/notification"
  val userFeedCreateEndPoint:String = "/private/user/feed/v1/create"


  val dbCourseBatchTable: String = config.getString("lms-cassandra.course_batch.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")

  val courseBatchdbReadCount = "db-course-batch-read-count"
  val notifiedUserCount = "notified-user-count"
  val skipNotifyUserCount = "skipped-notify-user-count"



  val userId: String = "userId"
  val courseId: String = "courseId"
  val batchId: String = "batchId"
  val notifyTemplate: String = "notifyTemplate"
  val firstName: String = "firstName"
  val trainingName: String = "TrainingName"
  val heldDate: String = "heldDate"
  val recipientUserIds: String = "recipientUserIds"
  val identifier: String = "identifier"
  val body: String = "body"
  val notificationSmsBody: String = "Congratulations! Download your course certificate from your profile page. If you have a problem downloading it on the mobile, update your DIKSHA app"
  val request: String = "request"
  val filters: String = "filters"
  val fields: String = "fields"
  val issued_certificates: String = "issued_certificates"
  val eData: String = "edata"
  val name: String = "name"
  val token: String = "token"
  val lastIssuedOn: String = "lastIssuedOn"
  val certificate: String = "certificate"
  val action: String = "action"
  val courseName: String = "courseName"
  val templateId: String = "templateId"
  val cert_templates: String = "cert_templates"
  val courseBatch: String = "CourseBatch"
  val l1: String = "l1"
  val id: String = "id"
  val data: String = "data"
  val category: String = "category"
  val certificates: String = "certificates"
  val priority: String = "priority"
  val userFeedMsg: String = "You have earned a certificate! Download it from your profile page."
  val priorityValue = 1

}
