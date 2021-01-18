package org.sunbird.collectioncomplete.domain

import java.util

import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any])  extends JobRequest(eventMap) {


  def action: String = readOrDefault[String]("edata.action", "")

  def batchId: String = readOrDefault[String]("edata.batchId", "")

  def courseId: String = readOrDefault[String]("edata.courseId", "")
  
  def userIds: List[String] = readOrDefault("edata.userIds", List[String]())

  def eData: util.Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]())

}
