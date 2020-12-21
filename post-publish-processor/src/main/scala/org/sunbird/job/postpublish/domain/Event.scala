package org.sunbird.job.postpublish.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  private val jobName = "PostPublishProcessor"
  import scala.collection.JavaConverters._
  def action: String = readOrDefault[String]("edata.action", "")

  def mimeType: String = readOrDefault[String]("edata.mimeType", "")

  def collectionId: String = readOrDefault[String]("edata.identifier", "")

  def eData: java.util.Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]())

  def validEvent(): Boolean = {
    StringUtils.equals("post-publish-process", action) &&
      StringUtils.equals("application/vnd.ekstep.content-collection", mimeType)
  }

}
