package org.sunbird.job.cspmigrator.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.domain.Event
import org.sunbird.job.cspmigrator.helpers.CSPMigrator
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.ServerException
import org.sunbird.job.helper.FailedEventHelper
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util
import java.util.UUID

class CSPMigratorFunction(config: CSPMigratorConfig, httpUtil: HttpUtil,
                          @transient var neo4JUtil: Neo4JUtil = null,
                          @transient var cassandraUtil: CassandraUtil = null,
                          @transient var cloudStorageUtil: CloudStorageUtil = null)
                         (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]], stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with CSPMigrator with FailedEventHelper {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[CSPMigratorFunction])
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
    logger.info("CSPMigratorFunction::processElement:: event context : " + event.context)
    logger.info("CSPMigratorFunction::processElement:: event edata : " + event.eData)

    val objMetadata: Map[String, AnyRef] = getMetadata(event.identifier, event.pkgVersion, event.status)(neo4JUtil)

    try {
      if (event.isValid(objMetadata, config)) {
        val migratedMetadataFields  = process(objMetadata, event.status, config, httpUtil, neo4JUtil, cassandraUtil)

        if(event.objectType.equalsIgnoreCase("Asset") && event.status.equalsIgnoreCase("Live")
          && (event.mimeType.equalsIgnoreCase("video/mp4") || event.mimeType.equalsIgnoreCase("video/webm"))) {
          pushStreamingUrlEvent(objMetadata, context)(metrics)
          metrics.incCounter(config.assetVideoStreamCount)
        }

        if(event.objectType.equalsIgnoreCase("Content")
          && (event.status.equalsIgnoreCase("Live") ||
          event.status.equalsIgnoreCase("Unlisted"))) {
          pushLiveNodePublishEvent(objMetadata, context, metrics)
          metrics.incCounter(config.liveNodePublishCount)
        } else updateMigrationVersion(objMetadata ++ migratedMetadataFields, event)(neo4JUtil)

        if(event.objectType.equalsIgnoreCase("Collection")
          && (event.status.equalsIgnoreCase("Live") ||
          event.status.equalsIgnoreCase("Unlisted"))) {
          pushLiveNodePublishEvent(objMetadata, context, metrics)
          metrics.incCounter(config.liveNodePublishCount)
        } else updateMigrationVersion(objMetadata ++ migratedMetadataFields, event)(neo4JUtil)

        logger.info("CSPMigratorFunction::processElement:: CSP migration operation completed for : " + event.identifier)
        metrics.incCounter(config.successEventCount)
      } else {
        logger.info("CSPMigratorFunction::processElement:: Event is not qualified for csp migration having identifier : " + event.identifier + " | objectType : " + event.objectType)
        metrics.incCounter(config.skippedEventCount)
      }
    } catch {
      case se: ServerException =>
        logger.error("CSPMigratorFunction :: Message processing failed for mid : " + event.mid() + " || " + event , se)
        logger.error("Error while migrating content :: " + se.getMessage)

        // Insert into neo4j with migrationVersion as 0.1
        if(!se.getMessage.contains("Migration Failed for"))
        neo4JUtil.updateNode(event.identifier, objMetadata + ("migrationVersion" -> 0.1.asInstanceOf[Number]))

        logger.info(s"""{ identifier: \"${objMetadata.getOrElse("identifier", "").asInstanceOf[String]}\", mimetype: \"${objMetadata.getOrElse("mimeType", "").asInstanceOf[String]}\", status: \"Failed\", stage: \"Static Migration\"}""")

//        val currentIteration = event.currentIteration
//        if (currentIteration < 1) {
//          val newEventMap = new util.HashMap[String, Any]()
//          newEventMap.putAll(event.getMap())
//          newEventMap.get("edata").asInstanceOf[util.HashMap[String, Any]].put("iteration",currentIteration+1)
//          pushEventForRetry(event.jobName, newEventMap, e, metrics, context)
//          logger.info("CSPMigratorFunction :: Failed Event Sent To Kafka Topic : " + config.kafkaFailedTopic + " | for mid : " + event.mid(), event)
//        }
//        else logger.info("CSPMigratorFunction :: Event Reached Maximum Retry Limit having mid : " + event.mid() + "| " +  event)
    }
  }
//
//  private def pushEventForRetry(jobName: String, newEventMap: util.HashMap[String, Any], error: Throwable, metrics: Metrics, context: ProcessFunction[Event, String]#Context): Unit = {
//    val failedEvent = getFailedEvent(jobName, newEventMap, error)
//    context.output(config.failedEventOutTag, failedEvent)
//    metrics.incCounter(config.errorEventCount)
//  }

  private def pushStreamingUrlEvent(objMetadata: Map[String, AnyRef], context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    val event = getStreamingEvent(objMetadata)
    context.output(config.generateVideoStreamingOutTag, event)
  }

  def getStreamingEvent(objMetadata: Map[String, AnyRef]): String = {
    val ets = System.currentTimeMillis
    val mid = s"""LP.$ets.${UUID.randomUUID}"""
    val channelId = objMetadata.getOrElse("channel", "").asInstanceOf[String]
    val ver = objMetadata.getOrElse("versionKey", "").asInstanceOf[String]
    val artifactUrl = objMetadata.getOrElse("artifactUrl", "").asInstanceOf[String]
    val contentType = objMetadata.getOrElse("contentType", "").asInstanceOf[String]
    val mimeType = objMetadata.getOrElse("mimeType", "").asInstanceOf[String]
    val status = objMetadata.getOrElse("status", "").asInstanceOf[String]
    val identifier = objMetadata.getOrElse("identifier", "").asInstanceOf[String]
    val pkgVersion = objMetadata.getOrElse("pkgVersion", "").asInstanceOf[Number]
    //TODO: deprecate using contentType in the event.
    val event = s"""{"eid":"BE_JOB_REQUEST", "ets": $ets, "mid": "$mid", "actor": {"id": "Live Video Stream Generator", "type": "System"}, "context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"}, "channel":"$channelId","env":"${config.jobEnv}"},"object":{"ver":"$ver","id":"$identifier"},"edata": {"action":"live-video-stream-generate","iteration":1,"identifier":"$identifier","channel":"$channelId","artifactUrl":"$artifactUrl","mimeType":"$mimeType","contentType":"$contentType","pkgVersion":$pkgVersion,"status":"$status"}}""".stripMargin
    logger.info(s"CSPMigratorFunction :: Asset Video Streaming Event for identifier $identifier  is  : $event")
    event
  }

  def pushLiveNodePublishEvent(objMetadata: Map[String, AnyRef], context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    val epochTime = System.currentTimeMillis
    val identifier = objMetadata.getOrElse("identifier", "").asInstanceOf[String]
    val pkgVersion = objMetadata.getOrElse("pkgVersion", "").asInstanceOf[Number]
    val objectType = objMetadata.getOrElse("objectType", "").asInstanceOf[String]
    val contentType = objMetadata.getOrElse("contentType", "").asInstanceOf[String]
    val mimeType = objMetadata.getOrElse("mimeType", "").asInstanceOf[String]
    val status = objMetadata.getOrElse("status", "").asInstanceOf[String]
    val publishType = if(status.equalsIgnoreCase("Live")) "Public" else "Unlisted"

    val event = s"""{"eid":"BE_JOB_REQUEST","ets":$epochTime,"mid":"LP.$epochTime.${UUID.randomUUID()}","actor":{"id":"content-republish","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"},"channel":"sunbird","env":"${config.jobEnv}"},"object":{"ver":"$pkgVersion","id":"$identifier"},"edata":{"publish_type":"$publishType","metadata":{"identifier":"$identifier", "mimeType":"$mimeType","objectType":"$objectType","lastPublishedBy":"System","pkgVersion":$pkgVersion},"action":"republish","iteration":1,"contentType":"$contentType"}}"""
    context.output(config.liveNodePublishEventOutTag, event)
    metrics.incCounter(config.liveNodePublishCount)
    logger.info("CSPMigratorFunction :: Live content publish triggered for " + identifier)
    logger.info("CSPMigratorFunction :: Live content publish event: " + event)
  }

  private def updateMigrationVersion(updatedMetadata: Map[String, AnyRef], event: Event)(neo4JUtil: Neo4JUtil): Unit = {
    logger.info(s"""CSPMigratorFunction:: process:: ${event.identifier} - ${event.objectType} updated fields data:: $updatedMetadata""")
    val updateMigrateData = updatedMetadata + ("migrationVersion" -> config.migrationVersion.asInstanceOf[AnyRef])
    neo4JUtil.updateNode(event.identifier, updateMigrateData)
    logger.info("CSPMigratorFunction:: process:: static fields migration completed for " + event.identifier)

  }

}
