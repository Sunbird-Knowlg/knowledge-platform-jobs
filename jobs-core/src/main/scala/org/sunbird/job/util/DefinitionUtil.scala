package org.sunbird.job.util

import org.slf4j.LoggerFactory
import scala.io.Source
import com.twitter.storehaus.cache.Cache
import com.twitter.util.Duration

object DefinitionUtil {

  private[this] val logger = LoggerFactory.getLogger(DefinitionUtil.getClass)
  val ttlMS: Long = 600000
  private var categoryDefinitionCache = Cache.ttl[String, Map[String, AnyRef]](Duration.fromMilliseconds(ttlMS))

  def get(objectType: String, version: String, basePath: String): Map[String, AnyRef] = {
    val key = getKey(objectType, version)
    val categoryDefinition = categoryDefinitionCache.getNonExpired(key).getOrElse(null)
    if (categoryDefinition == null) {
      logger.info(s"Getting Definition form Cloud for ObjectType: ${objectType} and version: ${version}.")
      val searchDefinition = getDefinition(basePath, objectType, version)
      put(objectType, version, searchDefinition)
      searchDefinition
    } else {
      logger.info("Found definition node in Cache.")
      categoryDefinition
    }
  }

  private def put(objectType: String, version: String, definitionNode: Map[String, AnyRef]): Unit = {
    val key = getKey(objectType, version)
    categoryDefinitionCache = categoryDefinitionCache.putClocked(key -> definitionNode)._2
  }

  private def getKey(objectType: String, version: String): String = {
    s"${objectType}:def_node:${version}"
  }

  private def getDefinition(basePath: String, objectType: String, version: String): Map[String, AnyRef] = {
    val path = s"${basePath}/${objectType.toLowerCase}/${version}/"
    val schemaMap: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](getFileToString(path, "schema.json"))
    val configMap: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](getFileToString(path, "config.json"))
    Map("schema" -> schemaMap, "config" -> configMap)
  }

  private def getFileToString(basePath: String, fileName: String): String = {
    Source.fromURL(basePath + fileName).mkString
  }
}
