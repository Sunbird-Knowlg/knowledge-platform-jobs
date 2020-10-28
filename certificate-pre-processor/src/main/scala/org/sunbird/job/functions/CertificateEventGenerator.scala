package org.sunbird.job.functions

import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.domain.{CertificateData, CourseDetails, Data, OrgDetails, Related, UserDetails}
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._

class CertificateEventGenerator(config: CertificatePreProcessorConfig)
                               (implicit val metrics: Metrics,
                                @transient var cassandraUtil: CassandraUtil = null) {

  lazy private val mapper: ObjectMapper = new ObjectMapper()

  def prepareGenerateEventEdata(edata: util.Map[String, AnyRef], collectionCache: DataCache): util.Map[String, AnyRef] = {
    println("prepareGenerateEventEdata called edata : " + edata)
    setIssuedCertificate(edata)
    setUserData(edata)
    setEventOrgData(edata)
    setCourseDetails(edata, collectionCache)
    setEventRelatedData(edata)
    setEventSvgData(edata)
    removeExtraData(edata)
    println("prepareGenerateEventEdata finished edata : " + edata)
    edata
  }

  private def setIssuedCertificate(edata: util.Map[String, AnyRef]) {
    println("setIssuedCertificate called edata : " + edata.toString)
    val issuedCertificatesResultMap = CertificateDbService.readUserCertificate(edata)(metrics, cassandraUtil, config)
    val issuedCertificates = issuedCertificatesResultMap.get(config.issued_certificates).asInstanceOf[util.ArrayList[util.Map[String, AnyRef]]]
    val issuedDate = issuedCertificatesResultMap.get(config.issuedDate).asInstanceOf[String]
    val certTemplate = edata.get(config.template).asInstanceOf[util.Map[String, AnyRef]]
    val certificates = issuedCertificates.asScala.filter((cert: util.Map[String, AnyRef]) => StringUtils.equalsIgnoreCase(cert.get(config.name).asInstanceOf[String], certTemplate.get(config.name).asInstanceOf[String])).toList
    if (certificates.nonEmpty && !edata.get(config.reIssue).asInstanceOf[Boolean]) {
      throw new Exception("Certificate is available for batchId : " + edata.get(config.batchId) +
        ", courseId : " + edata.get(config.courseId) + " and userId : " + edata.get(config.userId) + ". Not applied for reIssue.")
    }
    if (certificates.nonEmpty) {
      edata.put(config.oldId, certificates.head.getOrDefault(config.identifier, ""))
    }
    edata.putAll(mapper.readValue(mapper.writeValueAsString(CertificateData(issuedDate = issuedDate, basePath = config.certBasePath)), classOf[java.util.Map[String, AnyRef]]))
    println("setIssuedCertificate finish edata : " + edata.toString)
  }

  private def setUserData(edata: util.Map[String, AnyRef]) {
    println("setUserData called edata : " + edata.toString)
    val userResponse = CertificateApiService.getUserDetails(edata.get(config.userId).asInstanceOf[String])(config)
    println("setUserData userResponse : " + userResponse.toString)
    val userDetails = UserDetails(data = new util.ArrayList[Data]() {
      {
        add(Data(recipientId = edata.get(config.userId).asInstanceOf[String],
          recipientName = (userResponse.get(config.firstName) + " " + userResponse.get(config.lastName)).trim))
      }
    },
      orgId = userResponse.get(config.rootOrgId).asInstanceOf[String]
    )
    println("setUserData final userDetails : " + userDetails.toString)
    edata.putAll(mapper.readValue(mapper.writeValueAsString(userDetails), classOf[java.util.Map[String, AnyRef]]))
    println("setUserData finished edata : " + edata.toString)
  }

  private def setEventOrgData(edata: util.Map[String, AnyRef]) {
    println("setEventOrgData called edata : " + edata.toString)
    val keys = CertificateApiService.readOrgKeys(edata.get(config.orgId).asInstanceOf[String])(config)
    edata.putAll(mapper.readValue(mapper.writeValueAsString(OrgDetails(keys = keys)), classOf[java.util.Map[String, AnyRef]]))
    println("setEventOrgData finished edata : " + edata.toString)
  }

  private def setCourseDetails(edata: util.Map[String, AnyRef], collectionCache: DataCache) {
    println("setCourseDetails called edata : " + edata)
    val content = CertificateApiService.readContent(edata.get(config.courseId).asInstanceOf[String], collectionCache)(config, metrics)
    val courseDetails = CourseDetails(courseName = content.get(config.name).asInstanceOf[String],
      tag = edata.get(config.batchId).asInstanceOf[String])
    edata.putAll(mapper.readValue(mapper.writeValueAsString(courseDetails), classOf[java.util.Map[String, AnyRef]]))
    println("setCourseDetails finished edata : " + edata)
  }

  private def setEventRelatedData(edata: util.Map[String, AnyRef]) {
    println("setEventRelatedData called edata : " + edata)
    val related = Related(courseId = edata.get(config.courseId).asInstanceOf[String],
      batchId = edata.get(config.batchId).asInstanceOf[String])
    edata.put(config.related, mapper.readValue(mapper.writeValueAsString(related), classOf[java.util.Map[String, AnyRef]]))
    println("setEventRelatedData called edata : " + edata)
  }

  private def setEventSvgData(edata: util.Map[String, AnyRef]) {
    println("setEventSvgData called edata : " + edata)
    edata.putAll(edata.get(config.template).asInstanceOf[util.Map[String, AnyRef]])
    edata.remove(config.template)
    println("setEventSvgData finished edata : " + edata)
  }

  private def removeExtraData(edata: util.Map[String, AnyRef]): Unit = {
    if (edata.containsKey(config.reIssue))
      edata.remove(config.reIssue)
    edata.remove(config.courseId)
    edata.remove(config.batchId)
  }
}