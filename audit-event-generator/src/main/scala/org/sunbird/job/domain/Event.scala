package org.sunbird.job.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  private val jobName = "AuditEventGenerator"

  def action: String = readOrDefault[String]("edata.action", "")

  def eid: String = readOrDefault[String]("eid", "")

  def identifier: String = readOrDefault[String]("edata.identifier", "")

  def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

  def isValid: Boolean = {
    StringUtils.isNotBlank(mid())
  }

}
