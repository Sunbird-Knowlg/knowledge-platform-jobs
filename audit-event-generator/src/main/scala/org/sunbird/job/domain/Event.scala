package org.sunbird.job.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  private val jobName = "AuditEventGenerator"

  def operationType: String = readOrDefault("operationType", "")

  def nodeUniqueId: String = readOrDefault("nodeUniqueId", null)

  def objectType: String = readOrDefault[String]("objectType", null)


  def isValid: Boolean = {
    StringUtils.isNotBlank(objectType)
  }

}
