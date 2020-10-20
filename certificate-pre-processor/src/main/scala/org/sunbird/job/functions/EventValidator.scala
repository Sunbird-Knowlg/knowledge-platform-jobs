package org.sunbird.job.functions

import java.util

import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.task.CertificatePreProcessorConfig

import scala.collection.JavaConverters._

class EventValidator(config: CertificatePreProcessorConfig) {

  lazy private val mapper: ObjectMapper = new ObjectMapper()
  private[this] val logger = LoggerFactory.getLogger(classOf[EventValidator])

  def isValidEvent(edata: util.Map[String, AnyRef]): Boolean = {
    val action = edata.getOrDefault(config.action, "").asInstanceOf[String]
    val courseId = edata.getOrDefault(config.courseId, "").asInstanceOf[String]
    val batchId = edata.getOrDefault(config.batchId, "").asInstanceOf[String]
    val userIds = edata.getOrDefault(config.userIds, "").asInstanceOf[util.ArrayList[String]]

    StringUtils.equalsIgnoreCase(action, config.issueCertificate) &&
      StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(batchId) && CollectionUtils.isNotEmpty(userIds)
  }

  def validateTemplate(edata: util.Map[String, AnyRef])(implicit metrics: Metrics) {
    val template = edata.getOrDefault(config.template, new util.HashMap).asInstanceOf[util.Map[String, AnyRef]]
    if (MapUtils.isEmpty(template)) {
      logger.error("Certificate template is not available for batchId : " + edata.get(config.batchId).asInstanceOf[String] + " and courseId : " + edata.get(config.courseId).asInstanceOf[String])
      metrics.incCounter(config.skippedEventCount)
    }
  }

  def validateCriteria(template: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
    val criteriaString = template.get(config.criteria).asInstanceOf[String]
    if (StringUtils.isEmpty(criteriaString)) {
      throw new Exception("Certificate template has empty criteria: " + template.toString)
    }
    val criteria = mapper.readValue(criteriaString, classOf[util.Map[String, AnyRef]])
    if (MapUtils.isEmpty(criteria) && CollectionUtils.isEmpty(CollectionUtils.intersection(criteria.keySet(), config.certFilterKeys.asJava))) {
      throw new Exception("Certificate template has empty/invalid criteria: " + template.toString)
    }
    criteria
  }

}