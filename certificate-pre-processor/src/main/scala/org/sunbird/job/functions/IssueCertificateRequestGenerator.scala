package org.sunbird.job.functions

import java.util

import com.datastax.driver.core.Row
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._

class IssueCertificateRequestGenerator(config: CertificatePreProcessorConfig)
                                      (implicit val metrics: Metrics,
                                       @transient var cassandraUtil: CassandraUtil = null) {

  lazy private val mapper: ObjectMapper = new ObjectMapper()
  private[this] val logger = LoggerFactory.getLogger(classOf[IssueCertificateRequestGenerator])

  def prepareEventData(edata: util.Map[String, AnyRef], collectionCache: DataCache) {
    val certTemplates = fetchCertTemplates(edata)
    certTemplates.keySet().forEach(templateId => {
      //validate criteria
      val userIds = getUserIdsBasedOnCriteria(certTemplates.get(templateId).asInstanceOf[util.Map[String, AnyRef]], edata)
      //iterate over users and send to generate event method
      userIds.foreach(user => {
        generateCertificatesEvent(user, certTemplates.get(templateId).asInstanceOf[util.Map[String, AnyRef]], templateId)
      })
    })
  }

  private def fetchCertTemplates(edata: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
    val certTemplates = CertificateService.readCertTemplates(edata, config)(metrics, cassandraUtil)
    println("cert fetch success : " + certTemplates.toString)
    EventValidator.validateTemplate(certTemplates, edata, config)(metrics)
    certTemplates
  }

  private def getUserIdsBasedOnCriteria(template: util.Map[String, AnyRef], edata: util.Map[String, AnyRef]): List[String] = {
    println("getUserIdsBasedOnCriteria called")
    val criteria = EventValidator.validateCriteria(template, config)
    val enrollmentList = getUserFromEnrolmentCriteria(criteria.get(config.certFilterKeys(0)).asInstanceOf[util.Map[String, AnyRef]], edata, template)
    println("getUserIdsBasedOnCriteria : enrollmentList : " + enrollmentList)
    val assessmentList = getUsersFromAssessmentCriteria(criteria.get(config.certFilterKeys(1)).asInstanceOf[util.Map[String, AnyRef]], edata, template)
    println("getUserIdsBasedOnCriteria : assessmentList : " + assessmentList)
    val userList = getUsersFromUserCriteria(criteria.get(config.certFilterKeys(2)).asInstanceOf[util.Map[String, AnyRef]], edata,
      enrollmentList ++ assessmentList, template)
    println("getUserIdsBasedOnCriteria : userList : " + userList)
    userList
  }

  private def getUserFromEnrolmentCriteria(enrollmentCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], template: util.Map[String, AnyRef]): List[String] = {
    println("getUserFromEnrolmentCriteria called : " + enrollmentCriteria)
    if (MapUtils.isNotEmpty(enrollmentCriteria)) {
      val templateName = template.get(config.name).asInstanceOf[String]
      val rows = CertificateService.readUserIdsFromDb(enrollmentCriteria, edata, config)(metrics, cassandraUtil)
      val userIds = IssueCertificateUtil.getActiveUserIds(rows, edata, templateName, config)
      println("getUserFromEnrolmentCriteria active userids : " + userIds.toString)
      userIds
    } else List()
  }

  private def getUsersFromAssessmentCriteria(assessmentCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], template: util.Map[String, AnyRef]): List[String] = {
    println("getUsersFromAssessmentCriteria called : " + assessmentCriteria)
    if (MapUtils.isNotEmpty(assessmentCriteria)) {
      val rows = CertificateService.fetchAssessedUsersFromDB(edata, config)(metrics, cassandraUtil)
      val assessedUserIds = IssueCertificateUtil.getAssessedUserIds(rows, assessmentCriteria, edata, config)
      // ask in old we are checking userid we should ??
      if (assessedUserIds.nonEmpty) {
        val userIds = edata.get(config.userIds).asInstanceOf[util.List[String]].asScala.intersect(assessedUserIds).toList
        userIds
      } else {
        logger.info("No users satisfy assessment criteria for batchID: " + edata.get(config.batchId) + " and courseID: " + edata.get(config.courseId))
        List()
      }
    } else List()
  }

  private def getUsersFromUserCriteria(userCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], userIds: List[String], template: util.Map[String, AnyRef]): List[String] = {
    if (MapUtils.isNotEmpty(userCriteria)) {
      CertificateService.readUserIdsFromDb(userCriteria, edata, config)(metrics, cassandraUtil)
      )
      List()
    }
    else List()
  }

  private def generateCertificatesEvent(userId: String, template: util.Map[String, AnyRef], templateId: String): Unit = {
    println("generateCertificatesEvent called userId : " + userId)
    //call event generate
    //push event to next topic
  }

}
