package org.sunbird.job.functions

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, UUID}
import java.util.stream.Collectors

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import com.datastax.driver.core.{Row, TypeTokens}
import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.domain.{Certificate, Event, EventData, FailedEvent}
import org.sunbird.job.task.CertificateGeneratorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil}

import scala.collection.JavaConverters._

class PostCertificateProcessor(config: CertificateGeneratorConfig, httpUtil: HttpUtil)(@transient cassandraUtil: CassandraUtil) {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName)
  lazy private val mapper: ObjectMapper = new ObjectMapper()
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
  lazy private val gson = new Gson()


  def process(event: EventData, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    val certificate = event.certificate
    val courseId: String = event.courseId
    val userId: String = event.userId
    val batchId: String = event.batchId
    val primaryFields = Map(config.userId.toLowerCase() -> userId, config.batchId.toLowerCase -> batchId, config.courseId.toLowerCase -> courseId)
    val records = getIssuedCertificatesFromUserEnrollmentTable(primaryFields)
    if (records.nonEmpty)
      records.foreach((row: Row) => {
        val issuedOn = row.getTimestamp("completedOn")
        var certificatesList = row.getList(config.issued_certificates, TypeTokens.mapOf(classOf[String], classOf[String]))
        if (null == certificatesList && certificatesList.isEmpty) {
          certificatesList = new util.ArrayList[util.Map[String, String]]()
        }
        val updatedCerts: util.List[util.Map[String, String]] = certificatesList.stream().filter(cert => !StringUtils.equalsIgnoreCase(certificate.name, cert.get("name"))).collect(Collectors.toList())
        updatedCerts.add(new java.util.HashMap[String, String]() {
          {
            put(config.name, certificate.name)
            put(config.identifier, certificate.id)
            put(config.token, certificate.token)
            if (StringUtils.isNotBlank(certificate.lastIssuedOn))
              put(config.lastIssuedOn, certificate.lastIssuedOn)
          }
        })
        val query = getUpdateIssuedCertQuery(updatedCerts, userId, courseId, batchId, config)
        val result = cassandraUtil.update(query)
        if (result) {
          logger.info("issued certificates in user-enrollment table  updated successfully")
          metrics.incCounter(config.dbUpdateCount)
          val certificateAuditEvent = generateAuditEvent(userId, courseId, batchId, certificate)
          logger.info("pushAuditEvent: audit event generated for certificate : " + certificateAuditEvent)
          context.output(config.auditEventOutputTag, mapper.writeValueAsString(certificateAuditEvent))
          logger.info("pushAuditEvent: certificate audit event success")
          notifyUser(userId, event.courseName, issuedOn, courseId, batchId, event.templateId)(metrics)
          createUserFeed(userId, event.courseName, issuedOn, config)
          metrics.incCounter(config.successEventCount)
        } else {
          metrics.incCounter(config.failedEventCount)
          //          event.put("failInfo", FailedEvent("ERR_DB_UPDATION_FAILED", "db update failed"))
          //          context.output(config.failedEventOutputTag, gson.toJson(event))
          //          logger.info("Database update has failed {}", query)
        }

      })
  }

  /**
    * returns query for updating issued_certificates in user_enrollment table
    */
  def getUpdateIssuedCertQuery(updatedCerts: util.List[util.Map[String, String]], userId: String, courseId: String, batchId: String, config: CertificateGeneratorConfig):
  Update.Where = QueryBuilder.update(config.dbKeyspace, config.dbEnrollmentTable).where()
    .`with`(QueryBuilder.set(config.issued_certificates, updatedCerts))
    .where(QueryBuilder.eq(config.userId.toLowerCase, userId))
    .and(QueryBuilder.eq(config.courseId.toLowerCase, courseId))
    .and(QueryBuilder.eq(config.batchId.toLowerCase, batchId))


  private def getIssuedCertificatesFromUserEnrollmentTable(columns: Map[String, AnyRef])(implicit metrics: Metrics) = {
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


  private def createUserFeed(userId: String, courseName: String, issuedOn: Date,config: CertificateGeneratorConfig) {
    val req = s"""{"request":{"data":{"TrainingName":"$courseName","message":"${config.userFeedMsg}","heldDate":"${dateFormatter.format(issuedOn)}"},"category":"${config.certificates}","priority":${config.priorityValue} ,"userId":"${userId}"}}"""
    val url = config.learnerServiceBaseUrl + config.userFeedCreateEndPoint
    try {
      val response = httpUtil.post(url, req)
      if (response.status == 200) {
        logger.info("user feed response status {} :: {}", response.status, response.body)
      }
      else
        logger.info("user feed  response status {} :: {}", response.status, response.body)
    } catch {
      case e: Exception =>
        logger.error("Error while creating user feed : {}", userId + e)
    }
  }

  private def notifyUser(userId: String, courseName: String, issuedOn: Date, courseId: String, batchId: String, templateId: String)(implicit metrics: Metrics): Unit = {
    val userResponse: util.Map[String, AnyRef] = getUserDetails(userId) // call user Service
    val primaryFields = Map(config.courseId.toLowerCase() -> courseId, config.batchId.toLowerCase -> batchId)
    val row = getNotificationTemplates(primaryFields, metrics)
    if (userResponse != null) {
      val certTemplate = row.getMap(config.cert_templates, com.google.common.reflect.TypeToken.of(classOf[String]), TypeTokens.mapOf(classOf[String], classOf[String]))
      val url = config.learnerServiceBaseUrl + config.notificationEndPoint
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
          val response = httpUtil.post(url, mapper.writeValueAsString(request))
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
            val response = httpUtil.post(url, mapper.writeValueAsString(request))
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
      val httpResponse = httpUtil.post(config.learnerServiceBaseUrl + "/private/user/v1/search", userSearchRequest)
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

  private def generateAuditEvent(userId: String, courseId: String, batchId: String, certificate: Certificate): java.util.HashMap[String, AnyRef] = {
    //    CertificateAuditEvent(
    //      actor = Actor(id = userId),
    //      edata = EventData(props = Array("certificates"), `type` = "certificate-issued-svg"),
    //      context = EventContext(cdata = Array(Map("type" -> config.courseBatch, config.id -> batchId).asJava)),
    //      `object` = EventObject(id = certificate.get(config.id), `type` = "Certificate", rollup = Map[String, String](config.l1 -> courseId).asJava)
    //    )
    new java.util.HashMap[String, AnyRef]() {
      {
        put("eid", "BE_JOB_REQUEST")
        put("ets", System.currentTimeMillis().asInstanceOf[AnyRef])
        put("mid", s"LP.${System.currentTimeMillis()}.${UUID.randomUUID().toString}")
        put("ver", "3.0")
        put("actor", new java.util.HashMap[String, AnyRef]() {
          {
            put(config.id, userId)
            put("type", "User")
          }
        })
        put("context", new java.util.HashMap[String, AnyRef]() {
          {
            put("channel", "in.sunbird")
            put("env", "Course")
            put("pdata", new java.util.HashMap[String, AnyRef]() {
              {
                put("ver", "1.0")
                put(config.id, "org.sunbird.learning.platform")
                put("pid", "course-certificate-generator")
              }
            })
            put("cdata", new java.util.ArrayList[java.util.HashMap[String, AnyRef]]() {
              {
                add(new java.util.HashMap[String, AnyRef]() {
                  {
                    put(config.id, batchId)
                    put("type", config.courseBatch)
                  }
                })
              }
            })
          }
        })
        put("edata", new java.util.HashMap[String, AnyRef]() {
          {
            put("props", new java.util.ArrayList[String]() {
              {
                add("certificates")
              }
            })
            put("type", "certificate-issued-svg")
          }
        })
        put("object", new java.util.HashMap[String, AnyRef]() {
          {
            put(config.id, certificate.id)
            put("type", "Certificate")
            put("rollup", new java.util.HashMap[String, AnyRef]() {
              {
                put(config.l1, courseId)
              }
            })
          }
        })
      }
    }
  }


}
