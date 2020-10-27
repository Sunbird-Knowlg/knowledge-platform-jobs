package org.sunbird.job.functions

import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.stream.Collectors

import com.datastax.driver.core.Row
import com.datastax.driver.core.TypeTokens
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.{Actor, CertificateAuditEvent, EventContext, EventData, EventObject, FailedEvent}
import org.sunbird.job.task.{PostCertificateProcessorConfig, PostCertificateProcessorStreamTask}
import org.sunbird.job.util.CassandraUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._


class PostCertificateProcessFunction(config: PostCertificateProcessorConfig)
                                    (implicit val stringTypeInfo: TypeInformation[String],
                                     @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[PostCertificateProcessFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

  lazy private val mapper: ObjectMapper = new ObjectMapper()
  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
  lazy private val gson = new Gson()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def processElement(event: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    logger.info("event received {}", event)
    val eData = event.get(config.eData).asInstanceOf[java.util.Map[String, AnyRef]]
    if (isValidEvent(eData)) {
      val certificate = eData.get(config.certificate).asInstanceOf[util.Map[String, String]]
      val courseId: String = eData.get(config.courseId).asInstanceOf[String]
      val userId: String = eData.get(config.userId).asInstanceOf[String]
      val batchId: String = eData.get(config.batchId).asInstanceOf[String]
      val primaryFields = Map(config.userId.toLowerCase() -> userId, config.batchId.toLowerCase -> batchId, config.courseId.toLowerCase -> courseId)
      val records = getIssuedCertificatesFromUserEnrollmentTable(primaryFields, metrics)
      if (records.nonEmpty)
        records.foreach((row: Row) => {
          val issuedOn = row.getTimestamp("completedOn")
          var certificatesList = row.getList(config.issued_certificates, TypeTokens.mapOf(classOf[String], classOf[String]))
          if (null == certificatesList && certificatesList.isEmpty) {
            certificatesList = new util.ArrayList[util.Map[String, String]]()
          }
          val updatedCerts: util.List[util.Map[String, String]] = certificatesList.stream().filter(cert => !StringUtils.equalsIgnoreCase(certificate.get("name"), cert.get("name"))).collect(Collectors.toList())
          updatedCerts.add(new java.util.HashMap[String, String]() {
            {
              put(config.name, certificate.get(config.name))
              put(config.identifier, certificate.get(config.id))
              put(config.token, certificate.get(config.token))
              if (StringUtils.isNotBlank(certificate.get(config.lastIssuedOn)))
                put(config.lastIssuedOn, certificate.get(config.lastIssuedOn))
            }
          })
          val dataToSelect = new java.util.HashMap[String, AnyRef]() {
            {
              put(config.userId, eData.get(config.userId))
              put(config.courseId, eData.get(config.courseId))
              put(config.batchId, eData.get(config.batchId))
            }
          }
          val query = getUpdateIssuedCertQuery(updatedCerts, dataToSelect)
          val result = cassandraUtil.update(query)
          if (result) {
            logger.info("issued certificates in user-enrollment table  updated successfully")
            metrics.incCounter(config.dbUpdateCount)
            val certificateAuditEvent = generateAuditEvent(userId, courseId, batchId, certificate)
            logger.info("pushAuditEvent: audit event generated for certificate : " + certificateAuditEvent.`object`.id + " with mid : " + certificateAuditEvent.mid)
            context.output(config.auditEventOutputTag, gson.toJson(certificateAuditEvent))
            logger.info("pushAuditEvent: certificate audit event success")
            notifyUser(userId, eData.get(config.courseName).asInstanceOf[String], issuedOn, courseId, batchId, eData.get(config.templateId).asInstanceOf[String])(metrics)
            metrics.incCounter(config.successEventCount)
          } else {
            metrics.incCounter(config.failedEventCount)
            event.put("failInfo", FailedEvent("ERR_DB_UPDATION_FAILED", "db update failed"))
            context.output(config.failedEventOutputTag, gson.toJson(event))
            logger.info("Database update has failed {}", query)
          }

        })
      metrics.incCounter(config.totalEventsCount)
    }
  }

  /**
    * returns query for updating issued_certificates in user_enrollment table
    */
  def getUpdateIssuedCertQuery(updatedCerts: util.List[util.Map[String, String]], propertiesToSelect: util.Map[String, AnyRef]):
  Update.Where = QueryBuilder.update(config.dbKeyspace, config.dbEnrollmentTable).where()
    .`with`(QueryBuilder.set(config.issued_certificates, updatedCerts))
    .where(QueryBuilder.eq(config.userId.toLowerCase, propertiesToSelect.get(config.userId).asInstanceOf[String]))
    .and(QueryBuilder.eq(config.courseId.toLowerCase, propertiesToSelect.get(config.courseId).asInstanceOf[String]))
    .and(QueryBuilder.eq(config.batchId.toLowerCase, propertiesToSelect.get(config.batchId).asInstanceOf[String]))


  private def notifyUser(userId: String, courseName: String, issuedOn: Date, courseId: String, batchId: String, templateId: String)(implicit metrics: Metrics): Unit = {
    val userResponse: util.Map[String, AnyRef] = getUserDetails(userId) // call user Service
    val primaryFields = Map(config.courseId.toLowerCase() -> courseId, config.batchId.toLowerCase -> batchId)
    val row = getNotificationTemplates(primaryFields, metrics)
    if (userResponse != null) {
      val certTemplate = row.getMap(config.cert_templates, com.google.common.reflect.TypeToken.of(classOf[String]), TypeTokens.mapOf(classOf[String], classOf[String]))
      val url = config.learnerServiceBaseUrl + "/v2/notification"
      if (certTemplate != null && StringUtils.isNotBlank(templateId) && certTemplate.containsKey(templateId) && certTemplate.get(templateId).containsKey(config.notifyTemplate)) {
        logger.info("notification template is present in the cert-templates object {}", certTemplate.get(templateId).containsKey(config.notifyTemplate))
        val notifyTemplate = getNotificationTemplate(certTemplate.get(templateId))
        val request = new java.util.HashMap[String, AnyRef]() {
          {
            put(config.request, new util.HashMap[String, AnyRef]() {
              {
                putAll(notifyTemplate)
                put(config.firstName, userResponse.get(config.firstName).asInstanceOf[String])
                put(config.trainingName, courseName)
                put(config.heldDate, dateFormatter.format(issuedOn))
                put(config.recipientUserIds, util.Arrays.asList(userId))
                put(config.body, "email body")
              }
            })
          }
        }
        try {
          val response = PostCertificateProcessorStreamTask.httpUtil.post(url, mapper.writeValueAsString(request))
          if (response.status == 200) {
            metrics.incCounter(config.notifiedUserCount)
            logger.info("email response status {} :: {}", response.status, response.body)
          }
          else
            logger.info("email response status {} :: {}", response.status, response.body)
        } catch {
          case e: Exception =>
            logger.error("Error while sending email notification to user : " + userId, e)
        }
        if (userResponse.containsKey("maskedPhone") && StringUtils.isNoneBlank(userResponse.get("maskedPhone").asInstanceOf[String])) {
          request.put(config.body, "sms")
          val smsBody = config.notificationSmsBody.replaceAll("@@TRAINING_NAME@@", courseName).replaceAll("@@HELD_DATE@@", dateFormatter.format(issuedOn))
          request.put("body", smsBody)
          try {
            val response = PostCertificateProcessorStreamTask.httpUtil.post(url, mapper.writeValueAsString(request))
            if (response.status == 200)
              logger.info("phone response status {} :: {}", response.status, response.body)
            else
              logger.info("phone response status {} :: {}", response.status, response.body)
          } catch {
            case e: Exception =>
              logger.error("Error while sending phone notification to user : " + userId, e)
          }
        }
      } else {
        logger.info("notification template is not present in the cert-templates object {}")
        metrics.incCounter(config.skipNotifyUserCount)
      }

    }
  }

  private def getNotificationTemplate(certTemplate: util.Map[String, String]): util.Map[String, String] = {
    val notifyTemplate = certTemplate.get(config.notifyTemplate)
    if (notifyTemplate.isInstanceOf[String])
      try
        mapper.readValue(notifyTemplate, classOf[util.Map[String, String]])
      catch {
        case e: Exception =>
          logger.error("Error while fetching notify template : ", e)
          new util.HashMap[String, String]()
      }
    else notifyTemplate.asInstanceOf[util.Map[String, String]]
  }

  private def getIssuedCertificatesFromUserEnrollmentTable(columns: Map[String, AnyRef], metrics: Metrics) = {
    val selectWhere = QueryBuilder.select().all()
      .from(config.dbKeyspace, config.dbEnrollmentTable).
      where()
    columns.map(col => {
      col._2 match {
        case value: List[Any] =>
          selectWhere.and(QueryBuilder.in(col._1, value.asJava))
        case _ =>
          selectWhere.and(QueryBuilder.eq(col._1, col._2))
      }
    })
    metrics.incCounter(config.dbReadCount)
    cassandraUtil.find(selectWhere.toString).asScala.toList
  }

  /**
    * get notify template  from course-batch table
    *
    * @param columns
    * @param metrics
    * @return
    */
  private def getNotificationTemplates(columns: Map[String, AnyRef], metrics: Metrics): Row = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(config.dbKeyspace, config.dbCourseBatchTable).
      where()
    columns.map(col => {
      col._2 match {
        case value: List[Any] =>
          selectWhere.and(QueryBuilder.in(col._1, value.asJava))
        case _ =>
          selectWhere.and(QueryBuilder.eq(col._1, col._2))
      }
    })
    metrics.incCounter(config.dbReadCount)
    cassandraUtil.findOne(selectWhere.toString)
  }


  private def getUserDetails(userId: String): util.Map[String, AnyRef] = {
    logger.info("getting user info for id {}", userId)
    var content: util.Map[String, AnyRef] = null
    try {
      val userSearchRequest = prepareUserSearchRequest(userId)
      val httpResponse = PostCertificateProcessorStreamTask.httpUtil.post(config.learnerServiceBaseUrl + "/private/user/v1/search", userSearchRequest)
      if (httpResponse.status == 200) {
        logger.info("user search response status {} :: {} ", httpResponse.status, httpResponse.body)
        val response = mapper.readValue(httpResponse.body, classOf[java.util.Map[String, AnyRef]])
        val result = response.getOrDefault("result", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]].getOrDefault("response", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
        content = result.getOrDefault("content", new java.util.ArrayList[java.util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]].get(0)
      }
    }
    catch {
      case e: Exception =>
        logger.error("Error while searching for user : " + userId, e)
    }
    content
  }

  @throws[Exception]
  private def prepareUserSearchRequest(userId: String): String = {
    val request = new java.util.HashMap[String, AnyRef]() {
      {
        put(config.request, new java.util.HashMap[String, AnyRef]() {
          put(config.filters, new java.util.HashMap[String, AnyRef]() {
            {
              put(config.identifier, userId)
            }
          })
          put(config.fields, util.Arrays.asList("firstName", "lastName", "userName", "rootOrgName", "rootOrgId", "maskedPhone"))
        })

      }
    }
    mapper.writeValueAsString(request)
  }

  private def generateAuditEvent(userId: String, courseId: String, batchId: String, certificate: util.Map[String, String]): CertificateAuditEvent = {
    CertificateAuditEvent(
      actor = Actor(id = userId),
      edata = EventData(props = Array("certificates"), `type` = "certificate-issued-svg"),
      context = EventContext(cdata = Array(Map("type" -> config.courseBatch, config.id -> batchId).asJava)),
      `object` = EventObject(id = certificate.get(config.id), `type` = "Certificate", rollup = Map[String, String](config.l1 -> courseId).asJava)
    )
  }


  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.dbUpdateCount, config.dbReadCount, config.totalEventsCount, config.skipNotifyUserCount, config.notifiedUserCount)
  }

  private def isValidEvent(eData: java.util.Map[String, AnyRef]): Boolean = {
    val action = eData.getOrDefault(config.action, "").asInstanceOf[String]
    val certificate = eData.getOrDefault(config.certificate, "").asInstanceOf[util.Map[String, AnyRef]]
    val courseId: String = eData.get(config.courseId).asInstanceOf[String]
    val userId: String = eData.get(config.userId).asInstanceOf[String]
    val batchId: String = eData.get(config.batchId).asInstanceOf[String]
    StringUtils.equalsIgnoreCase(action, "post-process-certificate") &&
      MapUtils.isNotEmpty(certificate) && StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(userId) && StringUtils.isNotBlank(batchId)
  }

}
