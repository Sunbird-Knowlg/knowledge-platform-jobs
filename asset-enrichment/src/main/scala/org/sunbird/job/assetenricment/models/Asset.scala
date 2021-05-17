package org.sunbird.job.assetenricment.models

import java.util
import scala.collection.mutable

case class Asset(eventMap: util.Map[String, Any]) {

  val metadata: mutable.Map[String, AnyRef] = mutable.Map[String, AnyRef]()

  def putAll(data: Map[String, AnyRef]): Unit = metadata ++= data

  def put(key: String, value: AnyRef): Unit = metadata.put(key, value.asInstanceOf[AnyRef])

  def get(key: String, defaultValue: AnyRef): AnyRef = metadata.getOrElse(key, defaultValue)

  def getMetadata: Map[String, AnyRef] = metadata.toMap

  def artifactBasePath: String = metadata.getOrElse("artifactBasePath", "").asInstanceOf[String]

  def artifactUrl: String = metadata.getOrElse("artifactUrl", "").asInstanceOf[String]

  def identifier: String = metadata.getOrElse("IL_UNIQUE_ID", "").asInstanceOf[String]

  def mimeType: String = metadata.getOrElse("mimeType", "").asInstanceOf[String]

  def validate(contentUploadContextDriven: Boolean): Boolean = {
    contentUploadContextDriven && artifactBasePath.nonEmpty && artifactUrl.nonEmpty && artifactUrl.contains(artifactBasePath)
  }
}
