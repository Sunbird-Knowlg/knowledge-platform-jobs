package org.sunbird.job.functions

import java.util

import org.apache.commons.collections.MapUtils
import org.slf4j.LoggerFactory
import org.sunbird.collectioncomplete.domain.Event
import org.sunbird.job.Metrics
import org.sunbird.job.task.CollectionCompletePostProcessorConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

object CertificateUserUtil {

  private[this] val logger = LoggerFactory.getLogger(CertificateUserUtil.getClass)

  def getUserIdsBasedOnCriteria(template: Map[String, AnyRef], event: Event)
                               (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil,
                                config: CollectionCompletePostProcessorConfig): List[String] = {
    logger.info("getUserIdsBasedOnCriteria called")
    val criteria: util.Map[String, AnyRef] = EventValidator.validateCriteria(template, config)(metrics)
    val enrollmentList: List[String] = getUserFromEnrolmentCriteria(criteria.get(config.certFilterKeys.head).asInstanceOf[util.Map[String, AnyRef]], event, template)
    logger.info("getUserIdsBasedOnCriteria : enrollmentList : " + enrollmentList)
    val assessmentList: List[String] = getUsersFromAssessmentCriteria(criteria.get(config.certFilterKeys(1)).asInstanceOf[util.Map[String, AnyRef]], event, enrollmentList)
    logger.info("getUserIdsBasedOnCriteria : assessmentList : " + assessmentList)
    val userList: List[String] = getUsersFromUserCriteria(criteria.get(config.certFilterKeys(2)).asInstanceOf[util.Map[String, AnyRef]], assessmentList)
    logger.info("getUserIdsBasedOnCriteria : userList : " + userList)

    val templateFilterKeys: List[String] = (criteria.keySet().asScala.toList.intersect(config.certFilterKeys)).distinct
    val usersMap: Map[String, List[String]] = Map("enrollment" -> enrollmentList, "assessment" -> assessmentList, "user" -> userList)
    val usersToIssue = templateFilterKeys.filter(key => criteria.containsKey(key)).flatMap(key => usersMap.asJava.get(key).asInstanceOf[List[String]]).distinct
    logger.info("getUserIdsBasedOnCriteria finished usersToIssue : " + usersToIssue)
    usersToIssue
  }

  private def getUserFromEnrolmentCriteria(enrollmentCriteria: util.Map[String, AnyRef],
                                           event: Event, template: Map[String, AnyRef])
                                          (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil,
                                           config: CollectionCompletePostProcessorConfig): List[String] = {
    logger.info("getUserFromEnrolmentCriteria called : " + enrollmentCriteria)
    if (MapUtils.isNotEmpty(enrollmentCriteria) && !event.userIds.isEmpty) {
      val templateName = template.getOrElse(config.name, "").asInstanceOf[String]
      val userIds = CertificateDbService.readUserIdsFromDb(enrollmentCriteria, event, templateName)(metrics, cassandraUtil, config)
      logger.info("getUserFromEnrolmentCriteria active userids : " + userIds.toString)
      userIds
    } else List()
  }

  private def getUsersFromAssessmentCriteria(assessmentCriteria: util.Map[String, AnyRef], event: Event, enrolledUsers: List[String])
                                            (implicit metrics: Metrics, @transient cassandraUtil: CassandraUtil,
                                             config: CollectionCompletePostProcessorConfig): List[String] = {
    logger.info("getUsersFromAssessmentCriteria called : " + assessmentCriteria)
    if (MapUtils.isNotEmpty(assessmentCriteria) && !enrolledUsers.isEmpty) {
      val assessedUserIds = CertificateDbService.fetchAssessedUsersFromDB(event, assessmentCriteria, enrolledUsers)(metrics, cassandraUtil, config)
      // ask in old we are checking userid we should ??
      if (assessedUserIds.nonEmpty) {
        val userIds = enrolledUsers.intersect(assessedUserIds)
        userIds
      } else {
        logger.info("No users satisfy assessment criteria for batchID: " + event.batchId + " and courseID: " + event.courseId)
        List()
      }
    } else List()
  }

  private def getUsersFromUserCriteria(userCriteria: util.Map[String, AnyRef], userIds: List[String])
                                      (implicit config: CollectionCompletePostProcessorConfig): List[String] = {
    logger.info("getUsersFromUserCriteria called : " + userCriteria)
    if (MapUtils.isNotEmpty(userCriteria)) {
      val filteredUserIds = CertificateApiService.getUsersFromUserCriteria(userCriteria, userIds)(config)
      logger.info("getUsersFromUserCriteria : final filtered User : " + filteredUserIds)
      filteredUserIds
    }
    else List()
  }
}
