package org.sunbird.job.autocreatorv2.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.autocreatorv2.domain.Event
import org.sunbird.job.autocreatorv2.helpers.LearningObject
import org.sunbird.job.autocreatorv2.model.ObjectParent
import org.sunbird.job.autocreatorv2.util.CloudStorageUtil
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}

import java.util

class AutoCreatorV2Function(config: AutoCreatorV2Config, httpUtil: HttpUtil,
                            @transient var neo4JUtil: Neo4JUtil = null,
                            @transient var cassandraUtil: CassandraUtil = null,
                            @transient var cloudStorageUtil: CloudStorageUtil = null)
                           (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]], stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with BaseAutoCreatorV2 {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[AutoCreatorV2Function])

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
      /***
       * Construct the ObjectData object.
       * Check is it a collection type object and process children.
       * 1. Process cloud-storage data.
       * 2. Process external data.
       * 3. Process metadata.
        */
      logger.info(s"""Processing event with identifier: ${event.objectId}""")
      println(s"""Processing event with identifier: ${event.objectId}""")
      val definition = getDefinition(event.objectType)(config)
      val learningObject = new LearningObject(event.objectId, event.objectType, event.repository.get, event.downloadUrl)(config, httpUtil, definition)
      learningObject.process()(cloudStorageUtil)
      logger.info(s"Learning object constructed...")
      if (config.expandableObjects.contains(event.objectType)) {
        learningObject.processChildObj()(neo4JUtil, cassandraUtil, cloudStorageUtil, defCache)
      }
      learningObject.save()(neo4JUtil, cassandraUtil)
      context.output(config.linkCollectionOutputTag, ObjectParent(event.objectId, event.collection))
      logger.info("Bulk approval operation completed for : " + event.objectId)
      metrics.incCounter(config.successEventCount)
    } else {
      logger.info(s"Event is not qualified for auto-creation with identifier : ${event.objectId} | objectType : ${event.objectType} | source : ${event.repository}")
      metrics.incCounter(config.skippedEventCount)
    }
  }

}
