package org.sunbird.job.content.function

import akka.dispatch.ExecutionContexts
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.util.CloudStorageUtil
import org.sunbird.job.content.publish.domain.{Event, PublishMetadata}
import org.sunbird.job.content.publish.helpers.ContentPublisher
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}
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
  extends BaseProcessFunction[PublishMetadata, String](config) with ContentPublisher {

  private[this] val logger = LoggerFactory.getLogger(classOf[ContentPublishFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  private val readerConfig = ExtDataConfig(config.contentKeyspaceName, config.contentTableName)

  @transient var ec: ExecutionContext = _
  private val pkgTypes = List(EcarPackageType.FULL.toString, EcarPackageType.SPINE.toString, EcarPackageType.ONLINE.toString)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
    cloudStorageUtil = new CloudStorageUtil(config)
    ec = ExecutionContexts.global
    definitionCache = new DefinitionCache()
    definitionConfig = DefinitionConfig(config.schemaSupportVersionMap, config.definitionBasePath)
  }

  override def close(): Unit = {
    super.close()
    cassandraUtil.close()
  }

  override def metricsList(): List[String] = {
    List(config.contentPublishEventCount, config.contentPublishSuccessEventCount, config.contentPublishFailedEventCount, config.videoStreamingGeneratorEventCount, config.skippedEventCount)
  }

  override def processElement(data: PublishMetadata, context: ProcessFunction[PublishMetadata, String]#Context, metrics: Metrics): Unit = {
    logger.info("Content publishing started for : " + data.identifier)
    metrics.incCounter(config.contentPublishEventCount)
    val obj = getObject(data.identifier, data.pkgVersion, readerConfig)(neo4JUtil, cassandraUtil)
    val messages: List[String] = validate(obj, obj.identifier, validateMetadata)
    if (obj.pkgVersion > data.pkgVersion) {
      metrics.incCounter(config.skippedEventCount)
      logger.info(s"""pkgVersion should be greater than or equal to the obj.pkgVersion for : $obj.identifier""")
    } else {
      if (messages.isEmpty) {
        // Prepublish update
        updateProcessingNode(obj)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)
        // Clear redis cache
        val enrichedObj = enrichObject(obj)(neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig)
        val objWithEcar = getObjectWithEcar(enrichedObj, pkgTypes)(ec, cloudStorageUtil, definitionCache, definitionConfig, httpUtil)
        logger.info("Ecar generation done for Content: " + objWithEcar.identifier)
        saveOnSuccess(objWithEcar)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)
        pushStreamingUrlEvent(enrichedObj, context)(metrics)
        metrics.incCounter(config.contentPublishSuccessEventCount)
        logger.info("Content publishing completed successfully for : " + data.identifier)
      } else {
        saveOnFailure(obj, messages)(neo4JUtil)
        metrics.incCounter(config.contentPublishFailedEventCount)
        logger.info("Content publishing failed for : " + data.identifier)
      }
    }
  }

  private def pushStreamingUrlEvent(obj: ObjectData, context: ProcessFunction[PublishMetadata, String]#Context)(implicit metrics: Metrics): Unit = {
    if (config.isStreamingEnabled && config.streamableMimeType.contains(obj.mimeType)) {
      val event = getStreamingEvent(obj)
      context.output(config.generateVideoStreamingOutTag, event)
      metrics.incCounter(config.videoStreamingGeneratorEventCount)
    }
  }

  def getStreamingEvent(obj: ObjectData) : String = {
    val ets = System.currentTimeMillis
    val mid = s"""LP.${ets}.${UUID.randomUUID}"""
    val channelId = obj.getString("channel", "")
    val ver = obj.getString("versionKey", "")
    val artifactUrl = obj.getString("artifactUrl", "")
    // TODO: deprecate using contentType in the event.
    val event = s"""{"eid":"BE_JOB_REQUEST", "ets": ${ets}, "mid": "${mid}", "actor": {"id": "Post Publish Processor", "type": "System"}, "context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"}, "channel":"${channelId}","env":"${config.jobEnv}"},"object":{"ver":"${ver}","id":"${obj.identifier}"},"edata": {"action":"post-publish-process","iteration":1,"identifier":"${obj.identifier}","channel":"${channelId}","artifactUrl":"${artifactUrl}","mimeType":"${obj.mimeType}","contentType":"Resource","pkgVersion":1,"status":"Live"}}""".stripMargin
    logger.info(s"Video Streaming Event for identifier ${obj.identifier}  is  : ${event}")
    event
  }
}
