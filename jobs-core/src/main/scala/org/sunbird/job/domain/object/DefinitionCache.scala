package org.sunbird.job.domain.`object`

import com.twitter.storehaus.cache.Cache
import com.twitter.util.Duration
import org.slf4j.LoggerFactory
import org.sunbird.job.util.ScalaJsonUtil

import scala.io.Source

class DefinitionCache extends Serializable {

  private[this] val logger = LoggerFactory.getLogger(classOf[DefinitionCache])

  private var categoryDefinitionCache = Cache.ttl[String, ObjectDefinition](Duration.fromSeconds(600))

  def getDefinition(objectType: String, version: String, basePath: String): ObjectDefinition = {
    val key = getKey(objectType, version)
    categoryDefinitionCache.getNonExpired(key).getOrElse(prepareDefinition(basePath, objectType, version))
  }

  private def put(objectType: String, version: String, definition: ObjectDefinition): Unit = {
    val key = getKey(objectType, version)
    categoryDefinitionCache = categoryDefinitionCache.putClocked(key -> definition)._2
  }

  private def getKey(objectType: String, version: String): String = {
    s"${objectType}:def_node:${version}"
  }

  private def prepareDefinition(basePath: String, objectType: String, version: String): ObjectDefinition = {
    val path = s"${basePath}/${objectType.toLowerCase}/${version}/"
    val definition = try {
      val schemaMap: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](fileToString(path, "schema.json"))
      val configMap: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](fileToString(path, "config.json"))
      new ObjectDefinition(objectType, version, schemaMap, configMap)
    } catch {
      case ex: Exception =>  {
        ex.printStackTrace()
        logger.error(s"Error fetching definition from path : ${path}.", ex)
        throw new Exception("Error while fetching definition cache.", ex)
      }
    }
    if (definition != null) put(objectType, version, definition)
    definition
  }

  private def fileToString(basePath: String, fileName: String): String = {
    Source.fromURL(basePath + fileName).mkString
  }

}
