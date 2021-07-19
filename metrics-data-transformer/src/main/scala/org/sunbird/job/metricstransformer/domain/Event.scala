package org.sunbird.job.metricstransformer.domain

import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val jobName = "MetricsDataTransformer"

  def nodeUniqueId: String = readOrDefault("nodeUniqueId", "")

//  def transactionData: Map[String, AnyRef] = readOrDefault("transactionData", Map())

  def transactionData: Map[String, AnyRef] = readOrDefault("transactionData", Map())

//  def props: Map[String, AnyRef] = readOrDefault("transactionData", Map())

  def isValid: Boolean = {
//    event.contains(props)
    true
  }
}