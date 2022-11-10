package org.sunbird.job.cspmigrator.functions

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
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount, config.errorEventCount, config.assetVideoStreamCount, config.liveNodePublishCount)
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
    logger.info("CSPCassandraMigratorFunction::processElement:: event context : " + event.context)
    logger.info("CSPCassandraMigratorFunction::processElement:: event edata : " + event.eData)

    val objMetadata: Map[String, AnyRef] = getMetadata(event.identifier)(neo4JUtil)

    try {
        process(objMetadata, event.status, config, httpUtil, cassandraUtil, cloudStorageUtil)
        metrics.incCounter(config.successEventCount)
    } catch {
      case se: ServerException =>
        logger.error("CSPCassandraMigratorFunction :: Message processing failed for mid : " + event.mid() + " || " + event , se)
        logger.error("CSPCassandraMigratorFunction :: Error while migrating content :: " + se.getMessage)
        metrics.incCounter(config.failedEventCount)
        // Insert into neo4j with migrationVersion as 0.1
        if(!se.getMessage.contains("Migration Failed for"))
        neo4JUtil.updateNode(event.identifier, objMetadata + ("migrationVersion" -> 0.1.asInstanceOf[Number]))
    }
  }


}
