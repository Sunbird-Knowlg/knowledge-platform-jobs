package org.sunbird.job.functions

import java.util

import org.apache.commons.collections.MapUtils
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.CassandraUtil

class IssueCertificateRequestGenerator(config: CertificatePreProcessorConfig)
                                      (implicit val metrics: Metrics,
                                       @transient var cassandraUtil: CassandraUtil = null) {

  lazy private val mapper: ObjectMapper = new ObjectMapper()
  private[this] val logger = LoggerFactory.getLogger(classOf[IssueCertificateRequestGenerator])
  val certificateService = new CertificateService(config)(metrics, cassandraUtil)
  val eventValidator: EventValidator = new EventValidator(config)

  def prepareEventData(edata: util.Map[String, AnyRef], collectionCache: DataCache) {
    println("edata : " + edata)
    certTemplatesOperation(edata)
    //validate criteria as old issue
      //store asset into redis
    //    eventValidator.validateTemplate(edata)
    //iterate over users and send event to generate method
  }

  private def certTemplatesOperation(edata: util.Map[String, AnyRef]) {
    val certTemplates = certificateService.readCertTemplates(edata)
    logger.info("cert fetch success : " + certTemplates.toString)
    if (MapUtils.isEmpty(certTemplates)) {
      throw new Exception("Certificate template is not available for batchId : " + edata.get(config.batchId) + " and courseId : " + edata.get(config.courseId))
    }
    certTemplates.keySet().forEach(key => {
      val templateAsset = certificateService.readAsset(key)
      logger.info("asset read success : " + templateAsset)
      // update certTemplates based on asset response
      certTemplates.get(key).asInstanceOf[util.Map[String, AnyRef]].putAll(templateAsset)
      // fetch all the user based on criteria
      getUserIds(certTemplates.get(key).asInstanceOf[util.Map[String, AnyRef]], edata)
    })
    certificateService.updateCertTemplates(certTemplates, edata)
    logger.info("cert update success : " + certTemplates.toString)
  }

  private def getUserIds(template: util.Map[String, AnyRef], edata: util.Map[String, AnyRef]) {
    val criteria = eventValidator.validateCriteria(template)
    val enrollmentList = getUserFromEnrolmentCriteria(criteria.get(config.certFilterKeys(0)).asInstanceOf[util.Map[String, AnyRef]], edata, template)
    println("userIds : enrollmentList : " + enrollmentList)
    val assessmentList = getUsersFromAssessmentCriteria(criteria.get(config.certFilterKeys(1)).asInstanceOf[util.Map[String, AnyRef]], edata, template)
    val userList = getUsersFromUserCriteria(criteria.get(config.certFilterKeys(2)).asInstanceOf[util.Map[String, AnyRef]], edata, template, new util.ArrayList[String]() {
      {
        addAll(enrollmentList)
        addAll(assessmentList)
      }
    })
    generateCertificatesForEnrollment(enrollmentList)
  }

  private def getUserFromEnrolmentCriteria(enrollmentCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], template: util.Map[String, AnyRef]): util.ArrayList[String] = {
    if (MapUtils.isNotEmpty(enrollmentCriteria)) {
      certificateService.readUserIdsFromDb(enrollmentCriteria, edata, template.get(config.name).asInstanceOf[String])
    }
    new util.ArrayList[String]()
  }

  private def getUsersFromAssessmentCriteria(assessmentCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], template: util.Map[String, AnyRef]): util.ArrayList[String] = {
    //    if(MapUtils.isNotEmpty(assessmentCriteria)) {
    //      certificateService.readUserIdsFromDb(assessmentCriteria, edata, template.get(config.name).asInstanceOf[String])
    //    }
    new util.ArrayList[String]()
  }

  private def getUsersFromUserCriteria(userCriteria: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], template: util.Map[String, AnyRef], userIds: util.ArrayList[String]): util.ArrayList[String] = {
    //    if(MapUtils.isNotEmpty(userCriteria)) {
    //      certificateService.readUserIdsFromDb(userCriteria, edata, template.get(config.name).asInstanceOf[String])
    //    }
    new util.ArrayList[String]()
  }

  private def generateCertificatesForEnrollment(usersToIssue:util.ArrayList[String]): Unit ={
    usersToIssue.forEach(user => {
      println("userIds to issue certificate : " + usersToIssue)
    })
  }

}
