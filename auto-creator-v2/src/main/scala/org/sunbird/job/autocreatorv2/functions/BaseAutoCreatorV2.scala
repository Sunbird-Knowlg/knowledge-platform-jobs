package org.sunbird.job.autocreatorv2.functions

import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.task.AutoCreatorV2Config

trait BaseAutoCreatorV2 {

  lazy val defCache: DefinitionCache = new DefinitionCache()

  def getDefinition(objectType: String)(implicit config: AutoCreatorV2Config): ObjectDefinition = {
    val version = config.schemaSupportVersionMap.getOrElse(objectType.toLowerCase(), "1.0").asInstanceOf[String]
    defCache.getDefinition(objectType, version, config.definitionBasePath)
  }

}
