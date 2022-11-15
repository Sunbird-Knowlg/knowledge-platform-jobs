package org.sunbird.job.cspmigrator.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.domain.Event
import org.sunbird.job.cspmigrator.helpers.CSPNeo4jMigrator
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.ServerException
import org.sunbird.job.helper.FailedEventHelper
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util

class CSPNeo4jMigratorFunction(config: CSPMigratorConfig, httpUtil: HttpUtil,
                               @transient var neo4JUtil: Neo4JUtil = null,
                               @transient var cassandraUtil: CassandraUtil = null,
                               @transient var cloudStorageUtil: CloudStorageUtil = null)
                              (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]], stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with CSPNeo4jMigrator with FailedEventHelper {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[CSPNeo4jMigratorFunction])
  lazy val defCache: DefinitionCache = new DefinitionCache()

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount, config.errorEventCount, config.assetVideoStreamCount, config.liveContentNodePublishCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort)
    cloudStorageUtil = new CloudStorageUtil(config)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventsCount)
    logger.info("CSPNeo4jMigratorFunction::processElement:: event context : " + event.context)
    logger.info("CSPNeo4jMigratorFunction::processElement:: event edata : " + event.eData)

    val objMetadata: Map[String, AnyRef] = getMetadata(event.identifier)(neo4JUtil)

    try {
      if (event.isValid(objMetadata, config)) {
        val migratedMetadataFields = process(objMetadata, event.status, config, httpUtil, neo4JUtil, cassandraUtil)

        val migratedMap = objMetadata ++ migratedMetadataFields

//        context.output(config.cassandraMigrationOutputTag, event) ??

        if(config.videStreamRegenerationEnabled && event.objectType.equalsIgnoreCase("Asset") && event.status.equalsIgnoreCase("Live")
          && (event.mimeType.equalsIgnoreCase("video/mp4") || event.mimeType.equalsIgnoreCase("video/webm"))) {
          updateNeo4j(migratedMap + ("migrationVersion" -> config.migrationVersion.asInstanceOf[AnyRef]), event)(neo4JUtil)
          pushStreamingUrlEvent(migratedMap, context, config)(metrics)
          metrics.incCounter(config.assetVideoStreamCount)
        }

        if(config.liveNodeRepublishEnabled && (event.objectType.equalsIgnoreCase("Content") ||
          event.objectType.equalsIgnoreCase("Collection"))
          && (event.status.equalsIgnoreCase("Live") ||
          event.status.equalsIgnoreCase("Unlisted"))) {
          updateNeo4j(migratedMap, event)(neo4JUtil)
          pushLiveNodePublishEvent(objMetadata, context, metrics, config)
          metrics.incCounter(config.liveContentNodePublishCount)
        } else updateNeo4j(migratedMap + ("migrationVersion" -> config.migrationVersion.asInstanceOf[AnyRef]), event)(neo4JUtil)

        logger.info("CSPNeo4jMigratorFunction::processElement:: CSP migration operation completed for : " + event.identifier)
        metrics.incCounter(config.successEventCount)
      } else {
        logger.info("CSPNeo4jMigratorFunction::processElement:: Event is not qualified for csp migration having identifier : " + event.identifier + " | objectType : " + event.objectType)
        metrics.incCounter(config.skippedEventCount)
      }
    } catch {
      case se: Exception =>
        logger.error("CSPNeo4jMigratorFunction :: Message processing failed for mid : " + event.mid() + " || " + event , se)
        logger.error("CSPNeo4jMigratorFunction :: Error while migrating content :: " + se.getMessage)
        metrics.incCounter(config.failedEventCount)
        // Insert into neo4j with migrationVersion as 0.1
        neo4JUtil.updateNode(event.identifier, objMetadata + ("migrationVersion" -> 0.1.asInstanceOf[Number]))

        logger.info(s"""{ identifier: \"${objMetadata.getOrElse("identifier", "").asInstanceOf[String]}\", mimetype: \"${objMetadata.getOrElse("mimeType", "").asInstanceOf[String]}\", status: \"Failed\", stage: \"Static Migration\"}""")

//        val currentIteration = event.currentIteration
//        if (currentIteration < 1) {
//          val newEventMap = new util.HashMap[String, Any]()
//          newEventMap.putAll(event.getMap())
//          newEventMap.get("edata").asInstanceOf[util.HashMap[String, Any]].put("iteration",currentIteration+1)
//          pushEventForRetry(event.jobName, newEventMap, e, metrics, context)
//          logger.info("CSPNeo4jMigratorFunction :: Failed Event Sent To Kafka Topic : " + config.kafkaFailedTopic + " | for mid : " + event.mid(), event)
//        }
//        else logger.info("CSPNeo4jMigratorFunction :: Event Reached Maximum Retry Limit having mid : " + event.mid() + "| " +  event)
    }
  }
//
//  private def pushEventForRetry(jobName: String, newEventMap: util.HashMap[String, Any], error: Throwable, metrics: Metrics, context: ProcessFunction[Event, String]#Context): Unit = {
//    val failedEvent = getFailedEvent(jobName, newEventMap, error)
//    context.output(config.failedEventOutTag, failedEvent)
//    metrics.incCounter(config.errorEventCount)
//  }

}
