package org.sunbird.job.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  private val jobName = "AuditHistoryIndexer"

  def nodeType: String = readOrDefault("nodeType", "")

  def isValid: Boolean = {
    StringUtils.isNotBlank(nodeType)
  }

}
