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
    categoryDefinitionCache.getNonExpired(key).getOrElse(getDefinition(basePath, objectType, version))
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
    val definition: Map[String, AnyRef] = try {
      val schemaMap: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](fileToString(path, "schema.json"))
      val configMap: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](fileToString(path, "config.json"))
      Map("schema" -> schemaMap, "config" -> configMap)
    } catch {
      case ex: Exception =>  {
        ex.printStackTrace()
        logger.error(s"Error fetching definition from path : ${path}.", ex)
        Map()
      }
    }
    if (definition.nonEmpty) put(objectType, version, definition)
    definition
  }

  private def fileToString(basePath: String, fileName: String): String = {
    Source.fromURL(basePath + fileName).mkString
  }
}
