package org.sunbird.job.functions

import java.util

import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.domain.{CertificateData, CourseDetails, Data, OrgDetails, Related, UserDetails}
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

class CertificateEventGenerator(config: CertificatePreProcessorConfig)
                               (implicit val metrics: Metrics,
                                @transient var cassandraUtil: CassandraUtil = null) {

  def prepareGenerateEventEdata(edata: util.Map[String, AnyRef], collectionCache: DataCache): util.Map[String, AnyRef] = {
    println("prepareGenerateEventEdata called edata : " + edata)
    val eventEdata = new util.HashMap[String, AnyRef]()
    eventEdata.putAll(edata)
    eventEdata.putAll(setIssuedCertificate(edata))
    eventEdata.putAll(setUserData(edata))
    eventEdata.putAll(setEventOrgData(edata))
    eventEdata.putAll(setCourseDetails(edata, collectionCache))
    eventEdata.putAll(setEventRelatedData(edata))
    eventEdata.putAll(edata.get(config.template).asInstanceOf[util.Map[String, AnyRef]])
    eventEdata.remove(config.template)
    if (edata.containsKey(config.reIssue))
      eventEdata.remove(config.reIssue)
    eventEdata.remove(config.courseId)
    eventEdata.remove(config.batchId)
    println("prepareGenerateEventEdata finished edata : " + eventEdata)
    eventEdata
  }

  private def setIssuedCertificate(edata: util.Map[String, AnyRef])  = {
    println("setIssuedCertificate called edata : " + edata.toString)
    val issuedCertificatesResultMap = CertificateDbService.readUserCertificate(edata)(metrics, cassandraUtil, config)
    println("issuedCertificatesResultMap :: " + issuedCertificatesResultMap)
    val issuedCertificates = issuedCertificatesResultMap.get(config.issued_certificates).asInstanceOf[util.ArrayList[util.Map[String, AnyRef]]]
    val issuedDate = issuedCertificatesResultMap.get(config.issuedDate).asInstanceOf[String]
    val certTemplate = edata.get(config.template).asInstanceOf[util.Map[String, AnyRef]]
    val certificates = issuedCertificates.asScala.filter((cert: util.Map[String, AnyRef]) => StringUtils.equalsIgnoreCase(cert.get(config.name).asInstanceOf[String], certTemplate.get(config.name).asInstanceOf[String])).toList
    if (certificates.nonEmpty && !edata.get(config.reIssue).asInstanceOf[Boolean]) {
      throw new Exception("Certificate is available for batchId : " + edata.get(config.batchId) +
        ", courseId : " + edata.get(config.courseId) + " and userId : " + edata.get(config.userId) + ". Not applied for reIssue.")
    }
    val eventEdata = new util.HashMap[String, AnyRef]()
    if (certificates.nonEmpty) {
      eventEdata.put(config.oldId, certificates.head.getOrDefault(config.identifier, ""))
    }
    println("executed till here: #################" )
    eventEdata.putAll(convertToMap(CertificateData(issuedDate, config.certBasePath)))
    eventEdata
  }

  private def setUserData(edata: util.Map[String, AnyRef]) = {
    println("setUserData called edata : " + edata.toString)
    val userResponse = CertificateApiService.getUserDetails(edata.get(config.userId).asInstanceOf[String])(config)
    println("setUserData userResponse : " + userResponse.toString)
    val userDetails = UserDetails(data = new util.ArrayList[java.util.Map[String, AnyRef]]() {
      {
        add(convertToMap(Data(edata.get(config.userId).asInstanceOf[String],
          (userResponse.get(config.firstName) + " " + userResponse.get(config.lastName)).trim)))
      }
    },
      orgId = userResponse.get(config.rootOrgId).asInstanceOf[String]
    )
    println("setUserData final userDetails : " + userDetails.toString)
    convertToMap(userDetails)
  }

  private def setEventOrgData(edata: util.Map[String, AnyRef]) = {
    println("setEventOrgData called edata : " + edata.toString)
    val keys = CertificateApiService.readOrgKeys(edata.get(config.orgId).asInstanceOf[String])(config)
    if(MapUtils.isNotEmpty(keys)) {
      convertToMap(OrgDetails(keys))
    } else new java.util.HashMap[String, AnyRef]
  }

  private def setCourseDetails(edata: util.Map[String, AnyRef], collectionCache: DataCache) = {
    println("setCourseDetails called edata : " + edata)
    val content = CertificateApiService.readContent(edata.get(config.courseId).asInstanceOf[String], collectionCache)(config, metrics)
    val courseDetails = CourseDetails(courseName = content.get(config.name).asInstanceOf[String],
      tag = edata.get(config.batchId).asInstanceOf[String])
    convertToMap(courseDetails)
  }

  private def setEventRelatedData(edata: util.Map[String, AnyRef]) = {
    println("setEventRelatedData called edata : " + edata)
    val related = Related(courseId = edata.get(config.courseId).asInstanceOf[String],
      batchId = edata.get(config.batchId).asInstanceOf[String])
    new java.util.HashMap[String, AnyRef]() {{ put(config.related, convertToMap(related)) }}
  }

  def convertToMap(cc: AnyRef) = {
    JavaConverters.mapAsJavaMap(cc.getClass.getDeclaredFields.foldLeft (Map.empty[String, AnyRef]) { (a, f) => f.setAccessible(true)
      a + (f.getName -> f.get(cc)) })
  }
}