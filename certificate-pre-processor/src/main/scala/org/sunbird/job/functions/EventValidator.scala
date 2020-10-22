package org.sunbird.job.functions

import java.util

import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.sunbird.job.Metrics
import org.sunbird.job.task.CertificatePreProcessorConfig

import scala.collection.JavaConverters._

object EventValidator {

  lazy private val mapper: ObjectMapper = new ObjectMapper()

  def isValidEvent(edata: util.Map[String, AnyRef], config: CertificatePreProcessorConfig): Boolean = {
    val action = edata.getOrDefault(config.action, "").asInstanceOf[String]
    val courseId = edata.getOrDefault(config.courseId, "").asInstanceOf[String]
    val batchId = edata.getOrDefault(config.batchId, "").asInstanceOf[String]
    val userIds = edata.getOrDefault(config.userIds, "").asInstanceOf[util.ArrayList[String]]
    println(StringUtils.equalsIgnoreCase(action, config.issueCertificate) + " " + StringUtils.isNotBlank(courseId)
      + " " + StringUtils.isNotBlank(batchId) + " " + CollectionUtils.isNotEmpty(userIds))
    StringUtils.equalsIgnoreCase(action, config.issueCertificate) &&
      StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(batchId) && CollectionUtils.isNotEmpty(userIds)
  }

  def validateTemplate(certTemplates: util.Map[String, AnyRef], edata: util.Map[String, AnyRef],
                       config: CertificatePreProcessorConfig)(implicit metrics: Metrics) {
    println("validateTemplate called : " + certTemplates)
    if (MapUtils.isEmpty(certTemplates)) {
      metrics.incCounter(config.skippedEventCount)
      throw new Exception("Certificate template is not available for batchId : " + edata.get(config.batchId) + " and courseId : " + edata.get(config.courseId))
    }
  }

  def validateCriteria(template: util.Map[String, AnyRef], config: CertificatePreProcessorConfig)
                      (implicit metrics: Metrics): util.Map[String, AnyRef] = {
    println("validateCriteria called : " + template.get(config.criteria).asInstanceOf[String])
    val criteriaString = template.getOrDefault(config.criteria, "").asInstanceOf[String]
    if (StringUtils.isEmpty(criteriaString)) {
      metrics.incCounter(config.skippedEventCount)
      throw new Exception("Certificate template has empty criteria: " + template.toString)
    }
    val criteria = mapper.readValue(criteriaString, classOf[util.Map[String, AnyRef]])
    if (MapUtils.isEmpty(criteria) && CollectionUtils.isEmpty(CollectionUtils.intersection(criteria.keySet(), config.certFilterKeys.asJava))) {
      metrics.incCounter(config.skippedEventCount)
      throw new Exception("Certificate template has empty/invalid criteria: " + template.toString)
    }
    criteria
  }

  def isValidAssessUser(actualScore: Double, criteria: Map[String, AnyRef]): Boolean = {
    val operation = criteria.head._1
    val score = criteria.get(operation).asInstanceOf[Double]
    operation match {
      case "EQ" => actualScore == score
      case "eq" => actualScore == score
      case "=" => actualScore == score
      case ">" => actualScore > score
      case "<" => actualScore < score
      case ">=" => actualScore >= score
      case "<=" => actualScore <= score
      case "ne" => actualScore != score
      case "!=" => actualScore != score
      case _ => false
    }
  }

}