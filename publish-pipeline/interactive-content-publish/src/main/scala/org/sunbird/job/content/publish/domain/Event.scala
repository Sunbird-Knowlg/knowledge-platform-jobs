package org.sunbird.job.content.publish.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  val jobName = "content-publish"

  def eData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata",Map[String, AnyRef]())

  def action: String = readOrDefault[String]("edata.action", "")

  def publishType: String = readOrDefault[String]("edata.publish_type", "")

  def context: Map[String, AnyRef] = readOrDefault("context", Map[String, AnyRef]())

  def obj: Map[String, AnyRef] = readOrDefault("object",Map[String, AnyRef]())

  def publishChain : List[Map[String, AnyRef]]  = readOrDefault("edata.publishchain",List[Map[String, AnyRef]]()).map(m => m.toMap).toList

  def publishChainMetadata : String = readOrDefault[String]("edata.metadata.publishChainMetadata", "");

  def pkgVersion: Double = {
    val pkgVersion: Number = readOrDefault[Number]("edata.metadata.pkgVersion", 0)
    pkgVersion.doubleValue()
  }

  def validPublishChainEvent(): Boolean = {
    (StringUtils.equals("publishchain", action) && !publishChain.isEmpty)
  }
}
