package org.sunbird.job.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  private val jobName = "AutoCreatorV2"

  def eid: String = readOrDefault("eid", "")

  def isValid: Boolean = {
    StringUtils.isNotBlank(eid)
  }

}
