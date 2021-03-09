package org.sunbird.job.util

import org.slf4j.LoggerFactory
import scala.io.Source
import com.twitter.storehaus.cache.Cache
import com.twitter.util.Duration

class DefinitionUtil(ttlMS: Int) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DefinitionUtil])
  private var categoryDefinitionCache = Cache.ttl[String, Map[String, AnyRef]](Duration.fromSeconds(ttlMS))

  def get(objectType: String, version: String, basePath: String): Map[String, AnyRef] = {
    val key = getKey(objectType, version)
    val cacheDefinition = categoryDefinitionCache.getNonExpired(key).getOrElse(null)
    if (cacheDefinition == null) {
      val definition = getDefinition(basePath, objectType, version)
      if (definition.nonEmpty) put(objectType, version, definition)
      definition
    } else {
      cacheDefinition
    }
  }

  private def put(objectType: String, version: String, definition: Map[String, AnyRef]): Unit = {
    val key = getKey(objectType, version)
    categoryDefinitionCache = categoryDefinitionCache.putClocked(key -> definition)._2
  }

  private def getKey(objectType: String, version: String): String = {
    s"${objectType}:def_node:${version}"
  }

  private def getDefinition(basePath: String, objectType: String, version: String): Map[String, AnyRef] = {
    val path = s"${basePath}/${objectType.toLowerCase}/${version}/"
    try {
      val schemaMap: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](getFileToString(path, "schema.json"))
      val configMap: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](getFileToString(path, "config.json"))
      Map("schema" -> schemaMap, "config" -> configMap)
    } catch {
      case ex: Exception =>  {
        logger.info(s"Error fetching definition from path : ${path}.")
        Map()
      }
    }
  }

  private def getFileToString(basePath: String, fileName: String): String = {
    Source.fromURL(basePath + fileName).mkString
  }
}
