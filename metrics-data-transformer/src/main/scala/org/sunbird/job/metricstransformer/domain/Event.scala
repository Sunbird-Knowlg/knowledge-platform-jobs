package org.sunbird.job.metricstransformer.domain

import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val jobName = "MetricsDataTransformer"

  def nodeUniqueId: String = readOrDefault("nodeUniqueId", "")

  def channel: String = readOrDefault("channel","")

  def transactionData: Map[String, AnyRef] = readOrDefault("transactionData", Map())

}