package org.sunbird.job.util

import scala.io.Source

trait SearchDefinitionUtil {

  def searchDefinitionNode(basePath: String, objectType: String, version: String): Map[String, AnyRef] = {
    val path = s"${basePath}/${objectType.toLowerCase}/${version}/"
    val schemaMap: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](getFileToString(path, "schema.json"))
    val configMap: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](getFileToString(path, "config.json"))
    Map("schema" -> schemaMap, "config" -> configMap)
  }

  def getFileToString(basePath: String, fileName: String): String = {
    Source.fromURL(basePath + fileName).mkString
  }
}
