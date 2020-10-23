package org.sunbird.job.functions

import java.util

import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.domain.{CertificateData, CourseDetails, Data, OrgDetails, Related, UserDetails}
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._

class CertificateEventGenerator(config: CertificatePreProcessorConfig)
                               (implicit val metrics: Metrics,
                                @transient var cassandraUtil: CassandraUtil = null) {

  def prepareGenerateEventEdata(edata: util.Map[String, AnyRef], collectionCache: DataCache): util.Map[String, AnyRef] = {
    println("prepareGenerateEventEdata called : " + edata)
    //done
    setIssuedCertificate(edata)
    //done
    setUserData(edata)
    //done
    setEventOrgData(edata)
    //done
    setCourseDetails(edata, collectionCache)
    //done
    setEventRelatedData(edata)
    //done
    setEventSvgData(edata)
    if (edata.containsKey(config.reIssue))
      edata.remove(config.reIssue)
    println("prepareGenerateEventEdata called : " + edata)
    edata
  }

  private def setIssuedCertificate(edata: util.Map[String, AnyRef]) {
    println("setIssuedCertificate called edata : " + edata.toString)
    val issuedCertificatesResultMap = CertificateDbService.readUserCertificate(edata)(metrics, cassandraUtil, config)
    val issuedCertificates = issuedCertificatesResultMap.get(config.issued_certificates).asInstanceOf[util.ArrayList[util.Map[String, AnyRef]]]
    val issuedDate = issuedCertificatesResultMap.get(config.issuedDate).asInstanceOf[String]
    val certTemplate = edata.get(config.template).asInstanceOf[util.Map[String, AnyRef]]
    if (CollectionUtils.isNotEmpty(issuedCertificates) && edata.get(config.reIssue).asInstanceOf[Boolean]) {
      val certificates = issuedCertificates.asScala.filter((cert: util.Map[String, AnyRef]) => StringUtils.equalsIgnoreCase(cert.get(config.name).asInstanceOf[String], certTemplate.get(config.name).asInstanceOf[String])).toList
      //quetion : is alweays only one certificate will be there for one template
      val certData = CertificateData(issuedDate = issuedDate, basePath = "https://" + config.basePath + "/certs")
      if (edata.containsKey(config.reIssue) && edata.get(config.reIssue).asInstanceOf[Boolean])
        edata.put(config.oldId, certificates.head.getOrDefault(config.identifier, ""))
      edata.putAll(certData.getClass.getDeclaredFields.map(_.getName).zip(certData.productIterator.to).toMap.asInstanceOf[Map[String, AnyRef]].asJava)
      println("setIssuedCertificate finish edata : " + edata.toString)
    }
  }

  private def setUserData(edata: util.Map[String, AnyRef]) {
    println("setUserData called edata : " + edata.toString)
    val userResponse = CertificateApiService.getUserDetails(edata.get(config.userId).asInstanceOf[String])(config)
    val userDetails = UserDetails(data = new util.ArrayList[Data]() {
      {
        Data(recipientId = edata.get(config.userId).asInstanceOf[String],
          recipientName = (userResponse.get(config.firstName) + " " + userResponse.get(config.lastName)).trim)
      }
    },
      orgId = userResponse.get(config.rootOrgId).asInstanceOf[String]
    )
    edata.putAll(userDetails.getClass.getDeclaredFields.map(_.getName).zip(userDetails.productIterator.to).toMap.asInstanceOf[Map[String, AnyRef]].asJava)
    println("setUserData finished edata : " + edata.toString)
  }

  private def setEventOrgData(edata: util.Map[String, AnyRef]) {
    println("setEventOrgData called edata : " + edata.toString)
    val keys = CertificateApiService.readOrgKeys(edata.get(config.orgId).asInstanceOf[String])(config)
    val orgDetails = OrgDetails(keys = keys)
    edata.putAll(orgDetails.getClass.getDeclaredFields.map(_.getName).zip(orgDetails.productIterator.to).toMap.asInstanceOf[Map[String, AnyRef]].asJava)
    println("setEventOrgData finished edata : " + edata.toString)
  }

  private def setCourseDetails(edata: util.Map[String, AnyRef], collectionCache: DataCache) {
    println("setCourseDetails called edata : " + edata)
    val content = CertificateApiService.readContent(edata.get(config.courseId).asInstanceOf[String], collectionCache)(config)
    val courseDetails = CourseDetails(courseName = content.get(config.name).asInstanceOf[String],
      tag = edata.get(config.batchId).asInstanceOf[String])
    edata.putAll(courseDetails.getClass.getDeclaredFields.map(_.getName).zip(courseDetails.productIterator.to).toMap.asInstanceOf[Map[String, AnyRef]].asJava)
    println("setCourseDetails finished edata : " + edata)
  }

  private def setEventRelatedData(edata: util.Map[String, AnyRef]) {
    println("setEventRelatedData called edata : " + edata)
    val related = Related(courseId = edata.get(config.courseId).asInstanceOf[String],
      batchId = edata.get(config.batchId).asInstanceOf[String])
    edata.putAll(related.getClass.getDeclaredFields.map(_.getName).zip(related.productIterator.to).toMap.asInstanceOf[Map[String, AnyRef]].asJava)
    println("setEventRelatedData called edata : " + edata)
  }

  private def setEventSvgData(edata: util.Map[String, AnyRef]) {
    println("setEventSvgData called edata : " + edata)
    edata.putAll(edata.get(config.template).asInstanceOf[util.Map[String, AnyRef]])
    edata.remove(config.template)
    println("setEventSvgData finished edata : " + edata)
  }
}