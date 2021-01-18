package org.sunbird.job.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  private val jobName = "VideoStreamGenerator"
  import scala.collection.JavaConverters._
  def action: String = readOrDefault[String]("edata.action", "")

  def mimeType: String = readOrDefault[String]("edata.mimeType", "")

  def eid: String = readOrDefault[String]("eid", "")

  def artifactUrl: String = readOrDefault[String]("edata.artifactUrl", "")

  def identifier: String = readOrDefault[String]("edata.identifier", "")

  def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asScala.toMap

  def validEvent(): Boolean = {
    StringUtils.equals("post-publish-process", action) &&
      StringUtils.equals("application/vnd.ekstep.content-collection", mimeType)
  }

}
