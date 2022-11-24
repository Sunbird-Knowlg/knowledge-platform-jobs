package org.sunbird.job.autocreatorv2.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.autocreatorv2.domain.Event
import org.sunbird.job.autocreatorv2.helpers.AutoCreator
import org.sunbird.job.autocreatorv2.model.{ExtDataConfig, ObjectData, ObjectParent}
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util

class AutoCreatorFunction(config: AutoCreatorV2Config, httpUtil: HttpUtil,
                          @transient var neo4JUtil: Neo4JUtil = null,
                          @transient var cassandraUtil: CassandraUtil = null,
                          @transient var cloudStorageUtil: CloudStorageUtil = null)
                         (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]], stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with AutoCreator {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[AutoCreatorFunction])
  lazy val defCache: DefinitionCache = new DefinitionCache()

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName, config)
    cloudStorageUtil = new CloudStorageUtil(config)
  }

  override def close(): Unit = {
    super.close()
    cassandraUtil.close()
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventsCount)
    // TODO: Check if object already exists. If exists, add validation based on pkgVersion
    if (event.isValid) {
      logger.info("Processing event for bulk approval operation having identifier : " + event.objectId)
      logger.debug("event edata : " + event.eData)
      val definition: ObjectDefinition = defCache.getDefinition(event.objectType, config.schemaSupportVersionMap.getOrElse(event.objectType.toLowerCase(), "1.0").asInstanceOf[String], config.definitionBasePath)
      val obj: ObjectData = getObject(event.objectId, event.objectType, event.downloadUrl, event.repository)(config, httpUtil, definition)
      logger.debug("Constructed the ObjectData for " + obj.identifier)
      val enObj = enrichMetadata(obj, event.metadata)(config)
      logger.info("Enriched metadata for " + enObj.identifier)
      val updatedObj = processCloudMeta(enObj)(config, cloudStorageUtil, httpUtil)
      logger.info("Final updated metadata |with cloud-store updates| for " + updatedObj.identifier)
      val enrObj = if (config.expandableObjects.contains(updatedObj.objectType)) {
        val chMap: Map[String, AnyRef] = getChildren(updatedObj)(config)
        val filterChMap: Map[String, AnyRef] = filterChildren(chMap, updatedObj)
        val childrenObj: Map[String, ObjectData] = processChildren(filterChMap)(config, neo4JUtil, cassandraUtil, cloudStorageUtil, defCache, httpUtil)
        enrichHierarchy(updatedObj, childrenObj)(config)
      } else updatedObj
      val extConfig = ExtDataConfig(config.getString(enrObj.objectType.toLowerCase + ".keyspace", ""), definition.getExternalTable, definition.getExternalPrimaryKey, definition.getExternalProps)
      saveExternalData(enrObj.identifier, enrObj.extData.getOrElse(Map()), extConfig)(cassandraUtil)
      saveGraphData(enrObj.identifier, enrObj.metadata, definition)(neo4JUtil)
      context.output(config.linkCollectionOutputTag, ObjectParent(enrObj.identifier, event.collection))
      logger.info("Bulk approval operation completed for : " + event.objectId)
      metrics.incCounter(config.successEventCount)
    } else {
      logger.info("Event is not qualified for bulk approval having identifier : " + event.objectId + " | objectType : " + event.objectType + " | source : " + event.repository)
      metrics.incCounter(config.skippedEventCount)
    }
  }
}
