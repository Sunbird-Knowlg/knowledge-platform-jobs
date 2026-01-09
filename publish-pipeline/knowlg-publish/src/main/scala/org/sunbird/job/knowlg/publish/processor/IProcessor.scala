package org.sunbird.job.knowlg.publish.processor

import org.sunbird.job.util.CloudStorageUtil

abstract class IProcessor(basePath: String, identifier: String)(implicit cloudStorageUtil: CloudStorageUtil) {

  implicit val ss = cloudStorageUtil.getService

  val widgetTypeAssets: List[String] = List("js", "css", "json", "plugin")

  def process(ecrf: Plugin): Plugin

  def getBasePath(): String = basePath

  def getIdentifier(): String = identifier
}
