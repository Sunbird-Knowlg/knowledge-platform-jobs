package org.sunbird.job.compositesearch.domain

import java.util
import org.sunbird.job.domain.reader.JobRequest
import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  val jobName = "CompositeSearchIndexer"

  def index: String = readOrDefault("index", null)

  def operationType: String = readOrDefault("operationTyoe", "")

}
