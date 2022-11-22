package org.sunbird.job.cspmigrator.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.domain.Event
import org.sunbird.job.cspmigrator.helpers.CSPNeo4jMigrator
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.domain.`object`.DefinitionCache
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
        val migratedMetadataFields = process(objMetadata, config, httpUtil, cloudStorageUtil)

        val migratedMap = objMetadata ++ migratedMetadataFields

        event.objectType match {
          case "Asset"  =>
            event.mimeType.toLowerCase match {
              case "video/mp4" | "video/webm" =>
                finalizeMigration(migratedMap, event, metrics, config)(defCache, neo4JUtil)
                if (config.videStreamRegenerationEnabled && event.status.equalsIgnoreCase("Live")) {
                  logger.info("CSPNeo4jMigratorFunction :: Sending Asset For streaming URL generation: " + event.identifier)
                  pushStreamingUrlEvent(migratedMap, context, config)(metrics)
                  metrics.incCounter(config.assetVideoStreamCount)
                }
              case _ => finalizeMigration(migratedMap, event, metrics, config)(defCache, neo4JUtil)
            }
          case "Content" | "ContentImage" | "Collection" | "CollectionImage" =>
            event.mimeType.toLowerCase match {
              case "application/vnd.ekstep.ecml-archive" | "application/vnd.ekstep.content-collection" =>
                updateNeo4j(migratedMap, event)(defCache, neo4JUtil, config)
                logger.info("CSPNeo4jMigratorFunction :: Sending Content/Collection For cassandra migration: " + event.identifier)
                context.output(config.cassandraMigrationOutputTag, event)
              case _ =>
                finalizeMigration(migratedMap, event, metrics, config)(defCache, neo4JUtil)
                if(config.liveNodeRepublishEnabled && (event.status.equalsIgnoreCase("Live") ||
                  event.status.equalsIgnoreCase("Unlisted"))) {
                  pushLiveNodePublishEvent(objMetadata, context, metrics, config)
                  metrics.incCounter(config.liveContentNodePublishCount)
                }
            }
          case "AssessmentItem" =>
            updateNeo4j(migratedMap, event)(defCache, neo4JUtil, config)
            logger.info("CSPNeo4jMigratorFunction :: Sending AssessmentItem For cassandra migration: " + event.identifier)
            context.output(config.cassandraMigrationOutputTag, event)
          case _ => finalizeMigration(migratedMap, event, metrics, config)(defCache, neo4JUtil)
        }
      } else {
        logger.info("CSPNeo4jMigratorFunction::processElement:: Event is not qualified for csp migration having identifier : " + event.identifier + " | objectType : " + event.objectType)
        metrics.incCounter(config.skippedEventCount)
      }
    } catch {
      case se: Exception =>
        logger.error("CSPNeo4jMigratorFunction :: Message processing failed for mid : " + event.mid() + " || " + event , se)
        logger.error("CSPNeo4jMigratorFunction :: Error while migrating content :: " + se.getMessage)
        metrics.incCounter(config.failedEventCount)
        se.printStackTrace()
        logger.info(s"""{ identifier: \"${objMetadata.getOrElse("identifier", "").asInstanceOf[String]}\", mimetype: \"${objMetadata.getOrElse("mimeType", "").asInstanceOf[String]}\", status: \"Failed\", stage: \"Static Migration\"}""")
        // Insert into neo4j with migrationVersion as 0.1
        updateNeo4j(objMetadata + ("migrationVersion" -> 0.1.asInstanceOf[Number]), event)(defCache, neo4JUtil, config)

    }
  }


}
