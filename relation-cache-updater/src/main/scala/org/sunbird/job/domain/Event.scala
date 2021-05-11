package org.sunbird.job.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  val jobName = "RelationCacheUpdater"

  def identifier: String = readOrDefault[String]("edata.identifier", "")

  def action: String = readOrDefault[String]("edata.action", "")

  def mimeType: String = readOrDefault[String]("edata.mimeType", "")

  def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]


  def isValidEvent(allowedActions:List[String]): Boolean = {

    allowedActions.contains(action) && StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection") &&
      StringUtils.isNotBlank(identifier)
  }

}
