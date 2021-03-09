package org.sunbird.job.compositesearch.domain

import org.apache.commons.lang3.BooleanUtils
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  val jobName = "CompositeSearchIndexer"

  def index: Boolean = {
    val index = eventMap.get("index")
    if (index == null) true else {
      if (index.isInstanceOf[Boolean]) index.asInstanceOf[Boolean] else BooleanUtils.toBoolean(index.toString)
    }
  }

  def operationType: String = readOrDefault("operationType", "")

  def id: String = readOrDefault("nodeUniqueId", "")

  def nodeType: String = readOrDefault("nodeType", "")

}
