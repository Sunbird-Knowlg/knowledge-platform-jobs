package org.sunbird.job.transaction.domain

import org.apache.commons.lang3.{BooleanUtils, StringUtils}
import org.sunbird.job.domain.reader.JobRequest

import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.Date

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long)
    extends JobRequest(eventMap, partition, offset) {

  private val df: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

  private val IMAGE_SUFFIX = ".img"

  val jobName = "TransactionEventProcessor"

  def id: String = readOrDefault("nodeUniqueId", "")

  def operationType: String = readOrDefault("operationType", null)

  def nodeUniqueId: String = readOrDefault("nodeUniqueId", "")

  def createdOn: String = readOrDefault("createdOn", "")

  def channelId(channel: String): String = readOrDefault("channel", channel)

  def objectId: String = if (null != nodeUniqueId)
    nodeUniqueId.replaceAll(IMAGE_SUFFIX, "")
  else nodeUniqueId

  def objectType: String = readOrDefault[String]("objectType", null)

  def userId: String = readOrDefault[String]("userId", "")

  def transactionData: Map[String, AnyRef] = {
    readOrDefault("transactionData", new util.HashMap[String, AnyRef]())
      .asInstanceOf[Map[String, AnyRef]]
  }

  def nodeType: String = readOrDefault("nodeType", "")

  def ets: Long = readOrDefault("ets", 0L)

  def label: String = readOrDefault("label", "")

  def graphId: String = readOrDefault("graphId", "")

  def requestId: String = readOrDefault("requestId", "")

  def syncMessage: String = readOrDefault("syncMessage", null)

  def createdOnDate: Date =
    if (createdOn.isEmpty) new Date else df.parse(createdOn)

  def audit: Boolean = readOrDefault("audit", true)

  def transactionEventProcessorIsValid: Boolean = {
    StringUtils.isNotBlank(objectType) ||
    (operationType != null && null == syncMessage && audit)
  }

  def searchIndexerIsValid(restrictObjectTypes: util.List[String]): Boolean = {
    operationType != null && index && !restrictObjectTypes.contains(objectType)
  }

  def index: Boolean = {
    val index = eventMap.get("index")
    if (index == null) true
    else {
      index match {
        case bool: Boolean => bool
        case _             => BooleanUtils.toBoolean(index.toString)
      }
    }
  }

}
