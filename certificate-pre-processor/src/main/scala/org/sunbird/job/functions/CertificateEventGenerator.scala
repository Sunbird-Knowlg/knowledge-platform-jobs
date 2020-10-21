package org.sunbird.job.functions

import java.util

import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.domain.{CourseDetails, Data, OldId, OrgDetails, Related, UserDetails}
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._

class CertificateEventGenerator(config: CertificatePreProcessorConfig)
                               (implicit val metrics: Metrics,
                                @transient var cassandraUtil: CassandraUtil = null) {

  def prepareEventData(edata: util.Map[String, AnyRef], collectionCache: DataCache) {
    EventValidator.validateTemplate(edata,config)
    setIssuedCertificate(edata)
    setUserData(edata)
    setEventSvgData(edata)
    setEventOrgData(edata)
    setCourseDetails(edata, collectionCache)
    setEventRelatedData(edata)
    if (edata.containsKey(config.reIssue) && edata.get(config.reIssue).asInstanceOf[Boolean])
      setCertificateOldId(edata)
  }

  private def setIssuedCertificate(edata: util.Map[String, AnyRef]) {
    CertificateService.readUserCertificate(edata,config)(metrics, cassandraUtil)
    val issuedCertificates = edata.getOrDefault(config.issued_certificates, new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[util.ArrayList[util.Map[String, AnyRef]]]
    val certTemplate = edata.get(config.template).asInstanceOf[util.Map[String, AnyRef]]
    if (CollectionUtils.isNotEmpty(issuedCertificates) && edata.get(config.reIssue).asInstanceOf[Boolean]) {
      val certificates = issuedCertificates.asScala.filter((cert: util.Map[String, AnyRef]) => StringUtils.equalsIgnoreCase(cert.get(config.name).asInstanceOf[String], certTemplate.get(config.name).asInstanceOf[String])).toList
      certificates.map(cert => edata.putAll(cert))
    }
  }

  private def setUserData(edata: util.Map[String, AnyRef]) {
    val userResponse = CertificateService.getUserDetails(edata.get(config.userId).asInstanceOf[String],config)
    val userDetails = UserDetails(data = new util.ArrayList[Data]() {
      {
        Data(recipientId = edata.get(config.userId).asInstanceOf[String],
          recipientName = (userResponse.get(config.firstName) + " " + userResponse.get(config.lastName)).trim)
      }
    },
      orgId = userResponse.get(config.rootOrgId).asInstanceOf[String]
    )
    edata.putAll(userDetails.getClass.getDeclaredFields.map(_.getName).zip(userDetails.productIterator.to).toMap.asInstanceOf[Map[String, AnyRef]].asJava)
  }

  private def setEventSvgData(edata: util.Map[String, AnyRef]) {

  }

  private def setEventOrgData(edata: util.Map[String, AnyRef]) {
    val keys = CertificateService.readOrgKeys(edata.get(config.orgId).asInstanceOf[String],config)
    val orgDetails = OrgDetails(keys = keys)
    edata.putAll(orgDetails.getClass.getDeclaredFields.map(_.getName).zip(orgDetails.productIterator.to).toMap.asInstanceOf[Map[String, AnyRef]].asJava)
  }

  private def setCourseDetails(edata: util.Map[String, AnyRef], collectionCache: DataCache) {
    val content = CertificateService.readContent(edata.get(config.courseId).asInstanceOf[String], collectionCache,config)
    val courseDetails = CourseDetails(courseName = content.get(config.name).asInstanceOf[String])
    edata.putAll(courseDetails.getClass.getDeclaredFields.map(_.getName).zip(courseDetails.productIterator.to).toMap.asInstanceOf[Map[String, AnyRef]].asJava)
  }

  private def setEventRelatedData(edata: util.Map[String, AnyRef]) {
    val related = Related(courseId = edata.get(config.courseId).asInstanceOf[String],
      batchId = edata.get(config.batchId).asInstanceOf[String])
    edata.putAll(related.getClass.getDeclaredFields.map(_.getName).zip(related.productIterator.to).toMap.asInstanceOf[Map[String, AnyRef]].asJava)
  }

  private def setCertificateOldId(edata: util.Map[String, AnyRef]) {
    val oldId = OldId(oldId = edata.get(config.identifier).asInstanceOf[String])
    edata.putAll(oldId.getClass.getDeclaredFields.map(_.getName).zip(oldId.productIterator.to).toMap.asInstanceOf[Map[String, AnyRef]].asJava)
    edata.remove(config.identifier)
  }
}