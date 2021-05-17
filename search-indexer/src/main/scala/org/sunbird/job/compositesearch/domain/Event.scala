package org.sunbird.job.compositesearch.domain

import java.util
import org.apache.commons.lang3.BooleanUtils
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  val jobName = "SearchIndexer"

  def index: Boolean = {
    val index = eventMap.get("index")
    if (index == null) true else {
      if (index.isInstanceOf[Boolean]) index.asInstanceOf[Boolean] else BooleanUtils.toBoolean(index.toString)
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
