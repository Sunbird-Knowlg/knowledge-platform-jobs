package org.sunbird.job.functions

import java.util

import com.google.gson.Gson
import org.apache.commons.collections.MapUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.domain.{CertTemplate, CertificateGenerateEvent, EventObject}
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._

class IssueCertificateRequestGenerator(config: CertificatePreProcessorConfig)
                                      (implicit val metrics: Metrics,
                                       @transient var cassandraUtil: CassandraUtil = null) {

  lazy private val gson = new Gson()
  private[this] val logger = LoggerFactory.getLogger(classOf[IssueCertificateRequestGenerator])

  def prepareEventData(edata: util.Map[String, AnyRef], collectionCache: DataCache, context: ProcessFunction[util.Map[String, AnyRef], String]#Context) {
    val certTemplates = fetchCertTemplates(edata)
    certTemplates.keySet().forEach(templateId => {
      //validate criteria
      val certTemplate = certTemplates.get(templateId).asInstanceOf[util.Map[String, AnyRef]]
      val usersToIssue = getUserIdsBasedOnCriteria(certTemplate, edata)
      //iterate over users and send to generate event method
      val template = IssueCertificateUtil.prepareTemplate(certTemplate, config)
      usersToIssue.foreach(user => {
        generateCertificateEvent(user, template, edata, collectionCache, context)
      })
    })
  }

  private def fetchCertTemplates(edata: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
    val certTemplates = CertificateDbService.readCertTemplates(edata, config)(metrics, cassandraUtil)
    println("cert fetch success : " + certTemplates.toString)
    EventValidator.validateTemplate(certTemplates, edata, config)(metrics)
    certTemplates
  }

  private def getUserIdsBasedOnCriteria(template: util.Map[String, AnyRef], edata: util.Map[String, AnyRef]): List[String] = {
    println("getUserIdsBasedOnCriteria called")
    val criteria: util.Map[String, AnyRef] = EventValidator.validateCriteria(template, config)
    val enrollmentList: List[String] = getUserFromEnrolmentCriteria(criteria.get(config.certFilterKeys.head).asInstanceOf[util.Map[String, AnyRef]], edata, template)
    println("getUserIdsBasedOnCriteria : enrollmentList : " + enrollmentList)
    val assessmentList: List[String] = getUsersFromAssessmentCriteria(criteria.get(config.certFilterKeys(1)).asInstanceOf[util.Map[String, AnyRef]], edata, template)
    println("getUserIdsBasedOnCriteria : assessmentList : " + assessmentList)
    val userList: List[String] = getUsersFromUserCriteria(criteria.get(config.certFilterKeys(2)).asInstanceOf[util.Map[String, AnyRef]], enrollmentList ++ assessmentList)
    println("getUserIdsBasedOnCriteria : userList : " + userList)

    val templateFilterKeys: List[String] = (criteria.keySet().asScala ++ config.certFilterKeys).toList.distinct
    val usersMap: Map[String, List[String]] = Map("enrollment" -> enrollmentList, "assessment" -> assessmentList, "user" -> userList)
    val usersToIssue = templateFilterKeys.filter(key => criteria.containsKey(key)).flatMap(key => usersMap.get(key).asInstanceOf[List[String]]).distinct
    println("getUserIdsBasedOnCriteria usersToIssue : " + usersToIssue)
    usersToIssue
  }

  private def getUserFromEnrolmentCriteria(enrollmentCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], template: util.Map[String, AnyRef]): List[String] = {
    println("getUserFromEnrolmentCriteria called : " + enrollmentCriteria)
    if (MapUtils.isNotEmpty(enrollmentCriteria)) {
      val templateName = template.get(config.name).asInstanceOf[String]
      val rows = CertificateDbService.readUserIdsFromDb(enrollmentCriteria, edata, config)(metrics, cassandraUtil)
      val userIds = IssueCertificateUtil.getActiveUserIds(rows, edata, templateName, config)
      println("getUserFromEnrolmentCriteria active userids : " + userIds.toString)
      userIds
    } else List()
  }

  private def getUsersFromAssessmentCriteria(assessmentCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], template: util.Map[String, AnyRef]): List[String] = {
    println("getUsersFromAssessmentCriteria called : " + assessmentCriteria)
    if (MapUtils.isNotEmpty(assessmentCriteria)) {
      val rows = CertificateDbService.fetchAssessedUsersFromDB(edata, config)(metrics, cassandraUtil)
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

  private def getUsersFromUserCriteria(userCriteria: util.Map[String, AnyRef], userIds: List[String]): List[String] = {
    println("getUsersFromUserCriteria called : " + userCriteria)
    if (MapUtils.isNotEmpty(userCriteria)) {
      val filteredUserIds = CertificateApiService.getUsersFromUserCriteria(userCriteria, userIds, config)
      println("getUsersFromUserCriteria : final filtered User : " + filteredUserIds)
      filteredUserIds
    }
    else List()
  }

  private def generateCertificateEvent(userId: String, template: CertTemplate, edata: util.Map[String, AnyRef], collectionCache: DataCache, context :ProcessFunction[util.Map[String, AnyRef], String]#Context): Unit = {
    println("generateCertificatesEvent called userId : " + userId)
    val generateRequest = IssueCertificateUtil.prepareGenerateRequest(edata, template, userId, config)
    val edataRequest = generateRequest.getClass.getDeclaredFields.map(_.getName).zip(generateRequest.productIterator.to).toMap.asInstanceOf[Map[String, AnyRef]].asJava
    // generate certificate event edata
    val eventEdata = new CertificateEventGenerator(config).prepareGenerateEventEdata(edataRequest, collectionCache)
    println("generateCertificateEvent : eventEdata : " + eventEdata)
    pushEventToNextTopic(eventEdata, context)
  }

  private def pushEventToNextTopic(edata: util.Map[String, AnyRef], context: ProcessFunction[util.Map[String, AnyRef], String]#Context): Unit ={
    println("pushEventToNextTopic called : ")
    val certEvent: CertificateGenerateEvent = generateCertificateEvent(edata)
    println("pushEventToNextTopic certEvent send to next topic : " + certEvent)
    // send event to next topic to generate certificate
    context.output(config.generateCertificateOutputTag, gson.toJson(certEvent))
    logger.info("Certificate generate event successfully send to next topic")
    metrics.incCounter(config.successEventCount)
  }

  private def generateCertificateEvent(edata: util.Map[String, AnyRef]): CertificateGenerateEvent = {
    CertificateGenerateEvent(
      edata = edata,
      `object` = EventObject(id = edata.get(config.userId).asInstanceOf[String], `type` = "GenerateCertificate")
    )
  }
}
