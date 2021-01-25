package org.sunbird.notifier

import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.datastax.driver.core.{Row, TypeTokens}
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

case class NotificationMetaData(userId: String, courseName: String, issuedOn: Date, courseId: String, batchId: String, templateId: String)

class NotifierFunction(config: NotifierConfig, httpUtil: HttpUtil, @transient var cassandraUtil: CassandraUtil = null)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[NotificationMetaData, String](config) {

  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")

  private[this] val logger = LoggerFactory.getLogger(classOf[NotifierFunction])

  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

  lazy private val mapper: ObjectMapper = new ObjectMapper()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def processElement(metaData: NotificationMetaData,
                              context: ProcessFunction[NotificationMetaData, String]#Context,
                              metrics: Metrics): Unit = {

    val userResponse: util.Map[String, AnyRef] = getUserDetails(metaData.userId) // call user Service
    val primaryFields = Map(config.courseId.toLowerCase() -> metaData.courseId, config.batchId.toLowerCase -> metaData.batchId)
    val row = getNotificationTemplates(primaryFields, metrics)
    if (userResponse != null) {
      val certTemplate = row.getMap(config.cert_templates, com.google.common.reflect.TypeToken.of(classOf[String]), TypeTokens.mapOf(classOf[String], classOf[String]))
      val url = config.learnerServiceBaseUrl + config.notificationEndPoint
      if (certTemplate != null && StringUtils.isNotBlank(metaData.templateId) && certTemplate.containsKey(metaData.templateId) && certTemplate.get(metaData.templateId).containsKey(config.notifyTemplate)) {
        logger.info("notification template is present in the cert-templates object {}", certTemplate.get(metaData.templateId).containsKey(config.notifyTemplate))
        val notifyTemplate = getNotifyTemplateFromRes(certTemplate.get(metaData.templateId))
        val request = new java.util.HashMap[String, AnyRef]() {
          {
            put(config.request, new util.HashMap[String, AnyRef]() {
              {
                putAll(notifyTemplate)
                put(config.firstName, userResponse.get(config.firstName).asInstanceOf[String])
                put(config.trainingName, metaData.courseName)
                put(config.heldDate, dateFormatter.format(metaData.issuedOn))
                put(config.recipientUserIds, util.Arrays.asList(metaData.userId))
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
            logger.error("Error while sending email notification to user : " + metaData.userId, e)
        }
        if (userResponse.containsKey("maskedPhone") && StringUtils.isNoneBlank(userResponse.get("maskedPhone").asInstanceOf[String])) {
          request.put(config.body, "sms")
          val smsBody = config.notificationSmsBody.replaceAll("@@TRAINING_NAME@@", metaData.courseName).replaceAll("@@HELD_DATE@@", dateFormatter.format(metaData.issuedOn))
          request.put("body", smsBody)
          try {
            val response = httpUtil.post(url, mapper.writeValueAsString(request))
            if (response.status == 200)
              logger.info("phone response status {} :: {}", response.status, response.body)
            else
              logger.info("phone response status {} :: {}", response.status, response.body)
          } catch {
            case e: Exception =>
              logger.error("Error while sending phone notification to user : " + metaData.userId, e)
          }
        }
      } else {
        logger.info("notification template is not present in the cert-templates object {}")
        metrics.incCounter(config.skipNotifyUserCount)
      }

    }
  }


  private def getNotifyTemplateFromRes(certTemplate: util.Map[String, String]): util.Map[String, String] = {
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
    metrics.incCounter(config.courseBatchdbReadCount)
    cassandraUtil.findOne(selectWhere.toString)
  }


  private def getUserDetails(userId: String): util.Map[String, AnyRef] = {
    logger.info("getting user info for id {}", userId)
    var content: util.Map[String, AnyRef] = null
    try {
      val userSearchRequest = s"""{"request":{"filters":{"identifier":"${userId}"},"fields":["firstName","lastName","userName","rootOrgName","rootOrgId","maskedPhone"]}}"""
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


  override def metricsList(): List[String] = {
    List(config.courseBatchdbReadCount, config.skipNotifyUserCount, config.notifiedUserCount)
  }


}
