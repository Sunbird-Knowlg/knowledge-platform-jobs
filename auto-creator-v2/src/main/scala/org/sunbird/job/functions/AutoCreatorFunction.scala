package org.sunbird.job.functions

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.Event
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.helpers.AutoCreator
import org.sunbird.job.model.{ExtDataConfig, ObjectData}
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, JSONUtil, Neo4JUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

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
		neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
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
		if (event.isValid) {
			logger.info("Processing event for bulk approval operation having identifier : " + event.objectId)
            logger.info("event edata : " + event.eData)
			val definition: ObjectDefinition = defCache.getDefinition(event.objectType, config.schemaSupportVersionMap.getOrElse(event.objectType.toLowerCase(), "1.0").asInstanceOf[String], config.definitionBasePath)
            val downloadUrl = event.metadata.getOrElse("downloadUrl", "").asInstanceOf[String]
			val obj: ObjectData = getObject(event.objectId, event.objectType, downloadUrl, definition)(config)
			logger.info("graph metadata for "+obj.identifier + " : "+obj.metadata)
			val enObj = enrichMetadata(obj, event.metadata)(config)
			logger.info("enriched metadata for "+enObj.identifier + " : "+enObj.metadata)
			val updatedObj = processCloudMeta(enObj)(config, cloudStorageUtil)
			logger.info("final updated metadata for " + updatedObj.identifier + " : " + JSONUtil.serialize(updatedObj.metadata))
			val enrObj = if(config.expandableObjects.contains(updatedObj.objectType)) {
				val chMap: Map[String, AnyRef] = getChildren(updatedObj)(config)
				val childrenObj: List[ObjectData] = processChildren(chMap)(config, neo4JUtil, cassandraUtil, cloudStorageUtil, defCache)
				enrichHierarchy(updatedObj, childrenObj)(config)
			} else updatedObj
			val extConfig = ExtDataConfig(config.getString(enrObj.objectType.toLowerCase+"_keyspace", ""), definition.getExternalTable, definition.getExternalPrimaryKey, definition.getExternalProps)
			saveExternalData(enrObj.identifier, enrObj.extData.getOrElse(Map()), extConfig)(cassandraUtil)
			saveGraphData(enrObj.identifier, enrObj.metadata, definition)(neo4JUtil)
			linkCollection(enrObj.identifier, event.collection)(config, httpUtil)
		} else {
			logger.info("Event is not qualified for bulk approval having identifier : " + event.objectId + " | objectType : " + event.objectType + " | source : " + event.repository)
			metrics.incCounter(config.skippedEventCount)
		}
	}




}
