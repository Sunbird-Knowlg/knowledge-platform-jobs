package org.sunbird.job.functions

import java.util

import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.sunbird.collectioncomplete.domain.Event
import org.sunbird.job.Metrics
import org.sunbird.job.task.CollectionCompletePostProcessorConfig

import scala.collection.JavaConverters._

object EventValidator {

  lazy private val mapper: ObjectMapper = new ObjectMapper()

  def validateCriteria(template: Map[String, AnyRef], config: CollectionCompletePostProcessorConfig)
                      (implicit metrics: Metrics): util.Map[String, AnyRef] = {
    val criteriaString = template.getOrElse(config.criteria, "").asInstanceOf[String]
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
    val score = criteria.getOrElse(operation, 0.asInstanceOf[AnyRef]).asInstanceOf[Int].toDouble
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