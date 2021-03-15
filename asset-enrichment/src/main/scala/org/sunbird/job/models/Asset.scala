package org.sunbird.job.models

import java.util
import scala.collection.mutable

case class Asset(eventMap: util.Map[String, Any]) {

  val metaData: mutable.Map[String, AnyRef] = mutable.Map[String, AnyRef]()

  def setMetaData(data: Map[String, AnyRef]): Unit = metaData ++= data

  def artifactBasePath: String = metaData.getOrElse("artifactBasePath", "").asInstanceOf[String]

  def artifactUrl: String = metaData.getOrElse("artifactUrl", "").asInstanceOf[String]

  def identifier: String = metaData.getOrElse("IL_UNIQUE_ID", "").asInstanceOf[String]

  def addToMetaData(key: String, value: AnyRef): Unit = metaData.put(key, value.asInstanceOf[AnyRef])

  def getFromMetaData(key: String, defaultValue: AnyRef): AnyRef = metaData.getOrElse(key, defaultValue)

  def getMetaData: Map[String, AnyRef] = metaData.toMap

}
