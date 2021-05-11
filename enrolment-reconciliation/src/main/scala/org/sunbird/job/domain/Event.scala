package org.sunbird.job.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  val jobName = "EnrolmentReconciliation"

  def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

  def action: String = readOrDefault[String]("edata.action", "")

  def courseId: String = readOrDefault[String]("edata.courseId", "")

  def batchId: String = readOrDefault[String]("edata.batchId", "")

  def userId: String = readOrDefault[String]("edata.userId", "")

  def eType: String = readOrDefault[String]("edata.action", "")

  def isValidEvent(supportedEventType: String): Boolean = {

    StringUtils.equalsIgnoreCase(eType, supportedEventType)
  }

}
