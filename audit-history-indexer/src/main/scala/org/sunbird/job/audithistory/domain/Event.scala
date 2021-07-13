package org.sunbird.job.audithistory.domain

import org.sunbird.job.domain.reader.JobRequest

import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.Date

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val df:DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

  private val jobName = "AuditHistoryIndexer"

  def nodeType: String = readOrDefault("nodeType", "")

  def ets: Long = readOrDefault("ets", 0L)

  def userId: String = readOrDefault("userId", "")

  def nodeUniqueId: String = readOrDefault("nodeUniqueId", "")

  def objectType: String = readOrDefault("objectType", "")

  def label: String = readOrDefault("label", "")

  def graphId: String = readOrDefault("graphId", "")

  def requestId: String = readOrDefault("requestId", "")

  def transactionData: Map[String, AnyRef] = {
    readOrDefault("transactionData", new util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
  }

  def operationType: String = readOrDefault("operationType", null)

  def syncMessage: String = readOrDefault("syncMessage", null)

  def createdOn: String = readOrDefault("createdOn", "")

  def createdOnDate: Date = if (createdOn.isEmpty) new Date else df.parse(createdOn)

  def audit: Boolean = readOrDefault("audit", true)

  def isValid: Boolean = {
    operationType != null && null == syncMessage && audit
  }



}
