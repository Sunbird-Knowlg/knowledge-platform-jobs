package org.sunbird.job.certgen.functions

import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.datastax.driver.core.{Row, TypeTokens}
import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.certgen.task.CertificateGeneratorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class NotificationMetaData(userId: String, courseName: String, issuedOn: Date, courseId: String, batchId: String, templateId: String)

class NotifierFunction(config: CertificateGeneratorConfig, httpUtil: HttpUtil, @transient var cassandraUtil: CassandraUtil = null)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[NotificationMetaData, String](config) {

  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")

  private[this] val logger = LoggerFactory.getLogger(classOf[NotifierFunction])

  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

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

    val userResponse: Map[String, AnyRef] = getUserDetails(metaData.userId)(metrics) // call user Service
    if (null != userResponse && userResponse.nonEmpty) {
      val primaryFields = Map(config.courseId.toLowerCase() -> metaData.courseId, config.batchId.toLowerCase -> metaData.batchId)
      val row = getNotificationTemplates(primaryFields, metrics)
      val certTemplate = row.getMap(config.cert_templates, com.google.common.reflect.TypeToken.of(classOf[String]),
        TypeTokens.mapOf(classOf[String], classOf[String]))
      val url = config.learnerServiceBaseUrl + config.notificationEndPoint
      if (certTemplate != null && StringUtils.isNotBlank(metaData.templateId) &&
        certTemplate.containsKey(metaData.templateId) &&
        certTemplate.get(metaData.templateId).containsKey(config.notifyTemplate)) {
        logger.info("notification template is present in the cert-templates object {}",
          certTemplate.get(metaData.templateId).containsKey(config.notifyTemplate))
        val notifyTemplate = getNotifyTemplateFromRes(certTemplate.get(metaData.templateId))
        val request = mutable.Map[String, AnyRef]("request" -> (notifyTemplate ++ mutable.Map[String, AnyRef](
          config.firstName -> userResponse.getOrElse(config.firstName, "").asInstanceOf[String],
          config.trainingName -> metaData.courseName,
          config.heldDate -> dateFormatter.format(metaData.issuedOn),
          config.recipientUserIds -> List[String](metaData.userId),
          config.body -> "email body")))
        
        /*val request = s"""{"request": {${notifyTemplate}, "${config.firstName}": "${
          userResponse.get(config.firstName).asInstanceOf[String]
        }", "${config.trainingName}": "${metaData.courseName}", "${config.heldDate}": "${
          dateFormatter.format(metaData.issuedOn)
        }", ${config.recipientUserIds}:"[${metaData.userId}]", "${config.body}": "email body"}}"""*/
        val response = httpUtil.post(url, ScalaJsonUtil.serialize(request))
        if (response.status == 200) {
          metrics.incCounter(config.notifiedUserCount)
          logger.info("email response status {} :: {}", response.status, response.body)
        }
        else throw new Exception(s"Error in email response ${response}")
        if (StringUtils.isNoneBlank(userResponse.getOrElse("maskedPhone", "").asInstanceOf[String])) {
          request.put(config.body, "sms")
          val smsBody = config.notificationSmsBody.replaceAll("@@TRAINING_NAME@@", metaData.courseName)
            .replaceAll("@@HELD_DATE@@", dateFormatter.format(metaData.issuedOn))
          request.getOrElse("request", mutable.Map[String, AnyRef]()).asInstanceOf[mutable.Map[String, AnyRef]].put("body", smsBody)
          val response = httpUtil.post(url, ScalaJsonUtil.serialize(request))
          if (response.status == 200)
            logger.info("phone response status {} :: {}", response.status, response.body)
          else
            throw new Exception(s"email response status ${response} :: {}")
        }
      } else {
        logger.info("notification template is not present in the cert-templates object {}")
        metrics.incCounter(config.skipNotifyUserCount)
      }
    }
  }


  private def getNotifyTemplateFromRes(certTemplate: util.Map[String, String]): mutable.Map[String, String] = {
    val notifyTemplate = certTemplate.get(config.notifyTemplate)
    if (notifyTemplate.isInstanceOf[String]) ScalaJsonUtil.deserialize[mutable.Map[String, String]](notifyTemplate)
    else notifyTemplate.asInstanceOf[mutable.Map[String, String]]
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


  private def getUserDetails(userId: String)(metrics: Metrics): Map[String, AnyRef] = {
    logger.info("getting user info for id {}", userId)
    val httpResponse = httpUtil.get(config.learnerServiceBaseUrl + "/private/user/v1/read/" + userId)
    if (200 == httpResponse.status) {
      logger.info("user search response status {} :: {} ", httpResponse.status, httpResponse.body)
      val response = ScalaJsonUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
      val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("response", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      result
    } else if((400 == httpResponse.status) && httpResponse.body.contains("USER_ACCOUNT_BLOCKED")){
      logger.error("Error while fetching user details: " + httpResponse.status + " :: " + httpResponse.body)
      metrics.incCounter(config.skipNotifyUserCount)
      Map[String, AnyRef]()
    } else throw new Exception(s"Error while reading user for notification for userId: ${userId}")
  }

  override def metricsList(): List[String] = {
    List(config.courseBatchdbReadCount, config.skipNotifyUserCount, config.notifiedUserCount)
  }


}
