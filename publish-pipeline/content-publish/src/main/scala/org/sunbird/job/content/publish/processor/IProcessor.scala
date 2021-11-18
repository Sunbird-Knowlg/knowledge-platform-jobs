package org.sunbird.job.content.publish.processor

import org.sunbird.job.publish.util.CloudStorageUtil

abstract class IProcessor(basePath: String, identifier: String)(implicit cloudStorageUtil: CloudStorageUtil) {

  implicit val ss = cloudStorageUtil.getService

  val widgetTypeAssets: List[String] = List("js", "css", "json", "plugin")

  def process(ecrf: Plugin): Plugin

  def getBasePath(): String = basePath

  def getIdentifier(): String = identifier
}
