package org.sunbird.job.content.function

import akka.dispatch.ExecutionContexts
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.neo4j.driver.v1.exceptions.ClientException
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.content.publish.domain.Event
import org.sunbird.job.content.publish.helpers.{ContentPublisher, ExtractableMimeTypeHelper}
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.helper.FailedEventHelper
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, Neo4JUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type
import java.util.UUID
import scala.concurrent.ExecutionContext

class ContentPublishFunction(config: ContentPublishConfig, httpUtil: HttpUtil,
                             @transient var neo4JUtil: Neo4JUtil = null,
                             @transient var cassandraUtil: CassandraUtil = null,
                             @transient var cloudStorageUtil: CloudStorageUtil = null,
                             @transient var definitionCache: DefinitionCache = null,
                             @transient var definitionConfig: DefinitionConfig = null)
                            (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with ContentPublisher with FailedEventHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[ContentPublishFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  private var cache: DataCache = _
  private val readerConfig = ExtDataConfig(config.contentKeyspaceName, config.contentTableName)

  @transient var ec: ExecutionContext = _
  private val pkgTypes = List(EcarPackageType.FULL, EcarPackageType.SPINE)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
    cloudStorageUtil = new CloudStorageUtil(config)
    ec = ExecutionContexts.global
    definitionCache = new DefinitionCache()
    definitionConfig = DefinitionConfig(config.schemaSupportVersionMap, config.definitionBasePath)
    cache = new DataCache(config, new RedisConnect(config), config.nodeStore, List())
    cache.init()
  }

  override def close(): Unit = {
    super.close()
    cassandraUtil.close()
    cache.close()
  }

  override def metricsList(): List[String] = {
    List(config.contentPublishEventCount, config.contentPublishSuccessEventCount, config.contentPublishFailedEventCount,
      config.videoStreamingGeneratorEventCount, config.skippedEventCount, config.mvProcessorEventCount)
  }

  override def processElement(data: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    logger.info("Content publishing started for : " + data.identifier)
    metrics.incCounter(config.contentPublishEventCount)
    val obj: ObjectData = getObject(data.identifier, data.pkgVersion, data.mimeType, data.publishType, readerConfig)(neo4JUtil, cassandraUtil)
    try {
      if (obj.pkgVersion > data.pkgVersion) {
        metrics.incCounter(config.skippedEventCount)
        logger.info(s"""pkgVersion should be greater than or equal to the obj.pkgVersion for : ${obj.identifier}""")
      } else {
        val messages: List[String] = validate(obj, obj.identifier, config, validateMetadata)
        if (messages.isEmpty) {
          // Pre-publish update
          updateProcessingNode(new ObjectData(obj.identifier, obj.metadata ++ Map("lastPublishedBy" -> data.lastPublishedBy), obj.extData, obj.hierarchy))(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)

          val ecmlVerifiedObj = if (obj.mimeType.equalsIgnoreCase("application/vnd.ekstep.ecml-archive")) {
            val ecarEnhancedObj = ExtractableMimeTypeHelper.processECMLBody(obj, config)(ec, cloudStorageUtil)
            new ObjectData(obj.identifier, ecarEnhancedObj, obj.extData, obj.hierarchy)
          } else obj

          // Clear redis cache
          cache.del(data.identifier)
          val enrichedObj = enrichObject(ecmlVerifiedObj)(neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig)
          val objWithEcar = getObjectWithEcar(enrichedObj, if (enrichedObj.getString("contentDisposition", "").equalsIgnoreCase("online-only")) List(EcarPackageType.SPINE) else pkgTypes)(ec, neo4JUtil, cloudStorageUtil, config, definitionCache, definitionConfig, httpUtil)
          logger.info("Ecar generation done for Content: " + objWithEcar.identifier)
          saveOnSuccess(objWithEcar)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)
          pushStreamingUrlEvent(enrichedObj, context)(metrics)
          pushMVCProcessorEvent(enrichedObj, context)(metrics)
          metrics.incCounter(config.contentPublishSuccessEventCount)
          logger.info("Content publishing completed successfully for : " + data.identifier)
        } else {
          saveOnFailure(obj, messages, data.pkgVersion)(neo4JUtil)
          val errorMessages = messages.mkString("; ")
          pushFailedEvent(data, errorMessages, null, context)(metrics)
          logger.info("Content publishing failed for : " + data.identifier)
        }
      }
    } catch {
      case ex@(_: InvalidInputException | _: ClientException | _:java.lang.IllegalArgumentException) => // ClientException - Invalid input exception.
        ex.printStackTrace()
        saveOnFailure(obj, List(ex.getMessage), data.pkgVersion)(neo4JUtil)
        pushFailedEvent(data, null, ex, context)(metrics)
        logger.error("Error while publishing content :: " + ex.getMessage)
      case ex: Exception =>
        ex.printStackTrace()
        saveOnFailure(obj, List(ex.getMessage), data.pkgVersion)(neo4JUtil)
        logger.error(s"Error while processing message for Partition: ${data.partition} and Offset: ${data.offset}. Error : ${ex.getMessage}", ex)
        throw ex
    }
  }

  private def pushStreamingUrlEvent(obj: ObjectData, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    if (config.isStreamingEnabled && config.streamableMimeType.contains(obj.mimeType)) {
      val event = getStreamingEvent(obj)
      context.output(config.generateVideoStreamingOutTag, event)
      metrics.incCounter(config.videoStreamingGeneratorEventCount)
    }
  }

  def getStreamingEvent(obj: ObjectData): String = {
    val ets = System.currentTimeMillis
    val mid = s"""LP.$ets.${UUID.randomUUID}"""
    val channelId = obj.getString("channel", "")
    val ver = obj.getString("versionKey", "")
    val artifactUrl = obj.getString("artifactUrl", "")
    val contentType = obj.getString("contentType", "")
    val status = obj.getString("status", "")
    //TODO: deprecate using contentType in the event.
    val event = s"""{"eid":"BE_JOB_REQUEST", "ets": $ets, "mid": "$mid", "actor": {"id": "Post Publish Processor", "type": "System"}, "context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"}, "channel":"$channelId","env":"${config.jobEnv}"},"object":{"ver":"$ver","id":"${obj.identifier}"},"edata": {"action":"post-publish-process","iteration":1,"identifier":"${obj.identifier}","channel":"$channelId","artifactUrl":"$artifactUrl","mimeType":"${obj.mimeType}","contentType":"$contentType","pkgVersion":${obj.pkgVersion},"status":"$status"}}""".stripMargin
    logger.info(s"Video Streaming Event for identifier ${obj.identifier}  is  : $event")
    event
  }

  private def pushMVCProcessorEvent(obj: ObjectData, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    if (obj.getString("sourceURL", "").nonEmpty) {
      val event = getMVCProcessorEvent(obj)
      context.output(config.mvcProcessorTag, event)
      metrics.incCounter(config.mvProcessorEventCount)
    }
  }

  def getMVCProcessorEvent(obj: ObjectData): String = {
    val ets = System.currentTimeMillis
    val mid = s"""LP.$ets.${UUID.randomUUID}"""
    val channelId = obj.getString("channel", "")
    val ver = obj.getString("versionKey", "")
    val artifactUrl = obj.getString("artifactUrl", "")
    val name = obj.getString("name", "")
    //TODO: deprecate using contentType in the event.
    val event = s"""{"eid":"MVC_JOB_PROCESSOR", "ets": $ets, "mid": "$mid", "actor": {"id": "Post Publish Processor", "type": "System"}, "context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}, "channel":"$channelId","env":"${config.jobEnv}"},"object":{"ver":"$ver","id":"${obj.identifier}"},"eventData": {"action":"update-es-index","stage":1,"identifier":"${obj.identifier}","channel":"$channelId","artifactUrl":"$artifactUrl","name":"$name"}}""".stripMargin
    logger.info(s"MVC Processor Event for identifier ${obj.identifier}  is  : $event")
    event
  }

  private def pushFailedEvent(event: Event, errorMessage: String, error: Throwable, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    val failedEvent = if (error == null) getFailedEvent(event.jobName, event.getMap(), errorMessage) else getFailedEvent(event.jobName, event.getMap(), error)
    context.output(config.failedEventOutTag, failedEvent)
    metrics.incCounter(config.contentPublishFailedEventCount)
  }
}
