package org.sunbird.job.knowlg.publish.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.domain.reader.JobRequest

import java.util
import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  val jobName = "content-publish"

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

  def objectId: String = identifier
  
  def schemaVersion: String = readOrDefault[String]("edata.metadata.schemaVersion", "1.0")
  
  def getEventContext(): Map[String, AnyRef] = {
    val mid: String = readOrDefault[String]("mid", "")
    val requestId: String = readOrDefault[String]("edata.requestId", "")
    
    val defaultFeature = objectType match {
      case "Content" | "ContentImage" => "ContentPublish"
      case "Collection" | "CollectionImage" => "CollectionPublish"
      case "Question" | "QuestionImage" => "QuestionPublish"
      case "QuestionSet" | "QuestionSetImage" => "QuestionsetPublish"
      case _ => readOrDefault[String]("edata.action", "publish")
    }
    
    val featureName: String = readOrDefault[String]("edata.featureName", defaultFeature)
    
    Map("mid" -> mid, "requestId" -> requestId, "featureName" -> featureName)
  }

  def validEvent(config: KnowlgPublishConfig): Boolean = {
    ((StringUtils.equals("publish", action) && StringUtils.isNotBlank(identifier))
      && (config.supportedObjectType.contains(objectType) && config.supportedMimeType.contains(mimeType))
      && !StringUtils.equalsIgnoreCase("Asset", contentType))
  }
}
