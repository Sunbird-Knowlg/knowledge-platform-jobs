package org.sunbird.job.livenodepublisher.publish.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.livenodepublisher.task.LiveNodePublisherConfig
import org.sunbird.job.domain.reader.JobRequest

import java.util
import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  val jobName = "live-node-publisher"

  def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asScala.toMap

  def action: String = readOrDefault[String]("edata.action", "")

  def mimeType: String = readOrDefault[String]("edata.metadata.mimeType", "")

  def identifier: String = readOrDefault[String]("edata.metadata.identifier", "")

  def objectType: String = readOrDefault[String]("edata.metadata.objectType", "")

  def contentType: String = readOrDefault[String]("edata.contentType", "")

  def publishType: String = readOrDefault[String]("edata.publish_type", "")

  def lastPublishedBy: String = readOrDefault[String]("edata.metadata.lastPublishedBy", "")

  def pkgVersion: Double = {
    val pkgVersion: Number = readOrDefault[Number]("edata.metadata.pkgVersion", 0)
    pkgVersion.doubleValue()
  }

  def validEvent(config: LiveNodePublisherConfig): Boolean = {
    ((StringUtils.equals("republish", action) && StringUtils.isNotBlank(identifier))
      && (config.supportedObjectType.contains(objectType) && config.supportedMimeType.contains(mimeType))
      && !StringUtils.equalsIgnoreCase("Asset", contentType))
  }
}
