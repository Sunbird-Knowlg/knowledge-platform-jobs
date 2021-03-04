package org.sunbird.job.compositesearch.domain

import java.util

import org.apache.commons.lang3.BooleanUtils
import org.sunbird.job.domain.reader.JobRequest

import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  val jobName = "CompositeSearchIndexer"

  def index: Boolean = {
    readOrDefault("index", "true")
    val index = eventMap.get("index")
    if(index == null) true else {
      if(index.isInstanceOf[Boolean]) index.asInstanceOf[Boolean] else BooleanUtils.toBoolean(index.toString)
    }
  }

  def operationType: String = readOrDefault("operationTyoe", "")

}
