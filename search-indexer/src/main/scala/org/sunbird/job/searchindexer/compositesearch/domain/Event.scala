package org.sunbird.job.searchindexer.compositesearch.domain

import org.apache.commons.lang3.BooleanUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  val jobName = "SearchIndexer"

  def index: Boolean = {
    val index = eventMap.get("index")
    if (index == null) true else {
      index match {
        case bool: Boolean => bool
        case _ => BooleanUtils.toBoolean(index.toString)
      }
    }
  }

  def operationType: String = readOrDefault("operationType", "")

  def id: String = readOrDefault("nodeUniqueId", "")

  def nodeType: String = readOrDefault("nodeType", "")

  def objectType: String = readOrDefault("objectType", "")

  def validEvent(restrictObjectTypes: util.List[String]): Boolean = {
    (operationType != null) && index && (!restrictObjectTypes.contains(objectType))
  }

}
