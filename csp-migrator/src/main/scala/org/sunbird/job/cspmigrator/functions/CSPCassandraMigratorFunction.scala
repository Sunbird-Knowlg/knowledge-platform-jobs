package org.sunbird.job.cspmigrator.functions

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.domain.Event
import org.sunbird.job.cspmigrator.helpers.CSPCassandraMigrator
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.ServerException
import org.sunbird.job.helper.FailedEventHelper
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

import java.util

class CSPCassandraMigratorFunction(config: CSPMigratorConfig, httpUtil: HttpUtil,
                                @transient var neo4JUtil: Neo4JUtil = null,
                                @transient var cassandraUtil: CassandraUtil = null,
                                @transient var cloudStorageUtil: CloudStorageUtil = null)
                               (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]], stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with CSPCassandraMigrator with FailedEventHelper {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[CSPCassandraMigratorFunction])
  lazy val defCache: DefinitionCache = new DefinitionCache()

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount, config.errorEventCount, config.assetVideoStreamCount, config.liveContentNodePublishCount, config.liveQuestionNodePublishCount, config.liveQuestionSetNodePublishCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName, config)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort, config)
    cloudStorageUtil = new CloudStorageUtil(config)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventsCount)
    logger.info("CSPCassandraMigratorFunction::processElement:: event context : " + event.context)
    logger.info("CSPCassandraMigratorFunction::processElement:: event edata : " + event.eData)

    val objMetadata: Map[String, AnyRef] = getMetadata(event.identifier)(neo4JUtil)
    val identifier: String = objMetadata.getOrElse("identifier", "").asInstanceOf[String]

    try {
      val fieldsToMigrate: List[String] = if (config.getConfig.hasPath("neo4j_fields_to_migrate."+event.objectType.toLowerCase())) config.getConfig.getStringList("neo4j_fields_to_migrate."+event.objectType.toLowerCase()).asScala.toList
      else throw new ServerException("ERR_CONFIG_NOT_FOUND", "Fields to migrate configuration not found for objectType: " + event.objectType)
      val migratedMetadataFields: Map[String, String] =  fieldsToMigrate.flatMap(migrateField => {
        if(objMetadata.contains(migrateField)) {
          val metadataFieldValue = objMetadata.getOrElse(migrateField, "").asInstanceOf[String]

          val tempMetadataFieldValue = handleExternalURLS(metadataFieldValue, identifier, config, httpUtil, cloudStorageUtil)

          val migrateValue: String = if (StringUtils.isNotBlank(tempMetadataFieldValue))
            StringUtils.replaceEach(tempMetadataFieldValue, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
          else null
          if (config.copyMissingFiles && StringUtils.isNotBlank(migrateValue)) verifyFile(event.identifier, tempMetadataFieldValue, migrateValue, migrateField, config)(httpUtil, cloudStorageUtil)

          Map(migrateField -> migrateValue)
        } else Map.empty[String, String]
      }).filter(record => record._1.nonEmpty).toMap[String, String]

      val migratedObjMetadata = objMetadata ++ migratedMetadataFields
      logger.info("CSPCassandraMigratorFunction::processElement:: migratedObjMetadata : " + migratedObjMetadata)

      updateNeo4j(migratedObjMetadata, event)(defCache, neo4JUtil, config)

      process(migratedObjMetadata, config, httpUtil, cassandraUtil, cloudStorageUtil)

      event.objectType match {
        case "Content" | "Collection" =>
          finalizeMigration(migratedObjMetadata, event, metrics, config)(defCache, neo4JUtil)
          if(config.liveNodeRepublishEnabled && (event.status.equalsIgnoreCase("Live") ||
            event.status.equalsIgnoreCase("Unlisted"))) {
            pushLiveNodePublishEvent(objMetadata, context, metrics, config, config.liveCollectionNodePublishEventOutTag)
            metrics.incCounter(config.liveContentNodePublishCount)
          }
        case "QuestionSet"| "QuestionSetImage" =>
          finalizeMigration(migratedObjMetadata, event, metrics, config)(defCache, neo4JUtil)
          if(config.liveNodeRepublishEnabled && (event.status.equalsIgnoreCase("Live") ||
            event.status.equalsIgnoreCase("Unlisted"))) {
            pushQuestionPublishEvent(migratedObjMetadata, context, metrics, config, config.liveQuestionSetNodePublishEventOutTag, config.liveQuestionSetNodePublishCount)
          }
        case  _ => finalizeMigration(migratedObjMetadata, event, metrics, config)(defCache, neo4JUtil)
      }
    } catch {
      case se: Exception =>
        logger.error("CSPCassandraMigratorFunction :: Message processing failed for mid : " + event.mid() + " || " + event , se)
        logger.error("CSPCassandraMigratorFunction :: Error while migrating content :: " + se.getMessage)
        metrics.incCounter(config.failedEventCount)
        se.printStackTrace()
        logger.info(s"""{ identifier: \"${objMetadata.getOrElse("identifier", "").asInstanceOf[String]}\", mimetype: \"${objMetadata.getOrElse("mimeType", "").asInstanceOf[String]}\", status: \"Failed\", stage: \"Static Migration\"}""")
        // Insert into neo4j with migrationVersion as 0.1
        updateNeo4j(objMetadata + ("migrationVersion" -> 0.1.asInstanceOf[Number]), event)(defCache, neo4JUtil, config)
    }
  }


}
