package org.sunbird.job.cspmigrator.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.domain.Event
import org.sunbird.job.cspmigrator.helpers.CSPMigrator
import org.sunbird.job.cspmigrator.models.{ExtDataConfig, ObjectData}
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.ServerException
import org.sunbird.job.helper.FailedEventHelper
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util

class CSPMigratorFunction(config: CSPMigratorConfig, httpUtil: HttpUtil,
                          @transient var neo4JUtil: Neo4JUtil = null,
                          @transient var cassandraUtil: CassandraUtil = null,
                          @transient var cloudStorageUtil: CloudStorageUtil = null)
                         (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]], stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with CSPMigrator with FailedEventHelper {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[CSPMigratorFunction])
  lazy val defCache: DefinitionCache = new DefinitionCache()
  private val readerConfig = ExtDataConfig(config.contentKeyspaceName, config.contentTableName)

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount, config.errorEventCount)
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
    logger.info("CSPMigratorFunction::processElement:: event context : " + event.context)
    logger.info("CSPMigratorFunction::processElement:: event edata : " + event.eData)

    val obj: ObjectData = getObject(event.identifier, event.pkgVersion, event.mimeType, readerConfig)(neo4JUtil, cassandraUtil)

    try {
      if (event.isValid(obj.metadata, config)) {
            process(obj, config, readerConfig, event, neo4JUtil, cassandraUtil)
            logger.info("CSPMigratorFunction::processElement:: Content auto creator upload/approval operation completed for : " + event.identifier)
            metrics.incCounter(config.successEventCount)
      } else {
        logger.info("CSPMigratorFunction::processElement:: Event is not qualified for csp migration having identifier : " + event.identifier + " | objectType : " + event.objectType)
        metrics.incCounter(config.skippedEventCount)
      }
    } catch {
      case e: ServerException =>
        logger.error("CSPMigratorFunction :: Message processing failed for mid : " + event.mid() + " || " + event , e)
        val currentIteration = event.currentIteration
        if (currentIteration < 1) {
          val newEventMap = new util.HashMap[String, Any]()
          newEventMap.putAll(event.getMap())
          newEventMap.get("edata").asInstanceOf[util.HashMap[String, Any]].put("iteration",currentIteration+1)
          pushEventForRetry(event.jobName, newEventMap, e, metrics, context)
          logger.info("Failed Event Sent To Kafka Topic : " + config.kafkaFailedTopic + " | for mid : " + event.mid(), event)
        }
        else logger.info("Event Reached Maximum Retry Limit having mid : " + event.mid() + "| " +  event)
    }
  }



  private def pushEventForRetry(jobName: String, newEventMap: util.HashMap[String, Any], error: Throwable, metrics: Metrics, context: ProcessFunction[Event, String]#Context): Unit = {
    val failedEvent = getFailedEvent(jobName, newEventMap, error)
    context.output(config.failedEventOutTag, failedEvent)
    metrics.incCounter(config.errorEventCount)
  }


}
