package org.sunbird.job.domain

import org.apache.commons.lang3.{BooleanUtils, StringUtils}
import org.sunbird.job.domain.reader.JobRequest

import java.util

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  private val jobName = "AuditHistoryIndexer"

  def nodeType: String = readOrDefault("nodeType", "")

  def userId: String = readOrDefault("userId", "")

  def operationType: String = readOrDefault("operationType", "")

  def syncMessage: String = readOrDefault("syncMessage", null)

  def audit: Boolean = readOrDefault("audit", null)

  def shouldAudit:Boolean = if (null == audit) true else audit

  def isValid: Boolean = {
    operationType != null && null == syncMessage && shouldAudit
  }



}
