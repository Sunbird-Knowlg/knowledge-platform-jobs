package org.sunbird.job.util

import org.slf4j.LoggerFactory

class DefinitionUtil extends SearchDefinitionUtil {

  private[this] val logger = LoggerFactory.getLogger(classOf[DefinitionUtil])
  private var categoryDefinitionMap: Map[String, Map[String, AnyRef]] = Map[String, Map[String, AnyRef]]()

  def getDefinition(objectType: String, version: String, basePath: String): Map[String, AnyRef] = {
    val key = getKey(objectType, version)
    val categoryDefinition = categoryDefinitionMap.getOrElse(key, Map[String, AnyRef]())
    if (categoryDefinition.isEmpty) {
      logger.info("Getting Definition form Cloud.")
      val searchDefinition = searchDefinitionNode(basePath, objectType, version)
      putDefinition(objectType, version, searchDefinition)
      searchDefinition
    } else {
      logger.info("Found definition node in Cache.")
      categoryDefinition
    }
  }

  def putDefinition(objectType: String, version: String, definitionNode: Map[String, AnyRef]): Unit = {
    val key = getKey(objectType, version)
    categoryDefinitionMap += (key -> definitionNode)
  }

  private def getKey(objectType: String, version: String): String = {
  s"${objectType}:def_node:${version}"
  }

}
