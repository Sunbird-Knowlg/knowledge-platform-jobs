package org.sunbird.job.transaction.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val IMAGE_SUFFIX = ".img"
  private val jobName = "AuditEventGenerator"

  def id: String = readOrDefault("nodeUniqueId", "")

  def operationType: String = readOrDefault("operationType", "")

  def nodeUniqueId: String = readOrDefault("nodeUniqueId", null)

  def createdOn: String = read("createdOn").get

  def channelId(channel: String): String = readOrDefault("channel", channel)

  def objectId: String = if (null != nodeUniqueId) nodeUniqueId.replaceAll(IMAGE_SUFFIX, "") else nodeUniqueId

  def objectType: String = readOrDefault[String]("objectType", null)

  def userId: String = readOrDefault[String]("userId", "")

  def transactionData: Map[String, AnyRef] = readOrDefault("transactionData", Map[String, AnyRef]())

  def isValid: Boolean = {
    StringUtils.isNotBlank(objectType)
  }

}
