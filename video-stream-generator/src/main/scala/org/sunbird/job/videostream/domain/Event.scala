package org.sunbird.job.videostream.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val jobName = "VideoStreamGenerator"
  import scala.collection.JavaConverters._
  def action: String = readOrDefault[String]("edata.action", "")

  def mimeType: String = readOrDefault[String]("edata.mimeType", "")

  def eid: String = readOrDefault[String]("eid", "")

  def artifactUrl: String = readOrDefault[String]("edata.artifactUrl", "")

  def identifier(): String = readOrDefault[String]("edata.identifier", "")

  def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

  def isValid: Boolean = {
    StringUtils.isNotBlank(artifactUrl) &&
      StringUtils.isNotBlank(mimeType) &&
      (
        StringUtils.equalsIgnoreCase(mimeType, "video/mp4") ||
          StringUtils.equalsIgnoreCase(mimeType, "video/webm")
        ) &&
      StringUtils.isNotBlank(identifier)
  }

}
