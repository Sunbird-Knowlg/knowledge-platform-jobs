package org.sunbird.collectioncomplete.domain

import java.util

import org.sunbird.job.domain.reader.JobRequest
import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any])  extends JobRequest(eventMap) {

  def action: String = readOrDefault[String]("edata.action", "")

  def batchId: String = readOrDefault[String]("edata.batchId", "")

  def courseId: String = readOrDefault[String]("edata.courseId", "")

  def userIds: java.util.List[String] = readOrDefault[java.util.List[String]]("edata.userIds", new java.util.ArrayList[String]())

  def eData: util.Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]())

}
