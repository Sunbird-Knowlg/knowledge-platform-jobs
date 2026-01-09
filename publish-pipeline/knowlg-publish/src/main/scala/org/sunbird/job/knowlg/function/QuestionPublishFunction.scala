package org.sunbird.job.knowlg.function

import akka.dispatch.ExecutionContexts
import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.knowlg.publish.domain.{Event, PublishMetadata}
import org.sunbird.job.knowlg.publish.helpers.QuestionPublisher
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, LoggerUtil, Neo4JUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type
import scala.concurrent.ExecutionContext

class QuestionPublishFunction(config: KnowlgPublishConfig, httpUtil: HttpUtil,
                              @transient var neo4JUtil: Neo4JUtil = null,
                              @transient var cassandraUtil: CassandraUtil = null,
                              @transient var cloudStorageUtil: CloudStorageUtil = null,
                              @transient var definitionCache: DefinitionCache = null,
                              @transient var definitionConfig: DefinitionConfig = null)
                             (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with QuestionPublisher {

  private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPublishFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

  @transient var ec: ExecutionContext = _
  @transient var cache: DataCache = _
  private val pkgTypes = List(EcarPackageType.FULL.toString, EcarPackageType.ONLINE.toString)

  // Implement missing abstract methods from QuestionPublisher trait
  override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
    None
  }

  override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
    None
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort, config)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName, config)
    cloudStorageUtil = new CloudStorageUtil(config)
    ec = ExecutionContexts.global
    definitionCache = new DefinitionCache()
    definitionConfig = DefinitionConfig(config.schemaSupportVersionMap, config.definitionBasePath)
    cache = new DataCache(config, new RedisConnect(config), config.cacheDbId, List())
    cache.init()
  }

  override def close(): Unit = {
    super.close()
    cassandraUtil.close()
    cache.close()
  }

  override def metricsList(): List[String] = {
    List(config.questionPublishEventCount, config.questionPublishSuccessEventCount, config.questionPublishFailedEventCount)
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    val eventContext = event.getEventContext()
    val requestId = eventContext.getOrElse("requestId", "").asInstanceOf[String]
    val featureName = eventContext.getOrElse("featureName", "").asInstanceOf[String]
    
    // Convert Event to PublishMetadata for compatibility with async-questionset-publish logic
    val publishMetadata = PublishMetadata(
      eventContext, event.identifier, event.objectType, event.mimeType, 
      event.pkgVersion, event.publishType, event.lastPublishedBy, event.schemaVersion
    )
    
    // Delegate to the async-questionset-publish logic
    processPublishMetadata(publishMetadata, context, metrics)
  }
  
  private def processPublishMetadata(data: PublishMetadata, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    val requestId = data.eventContext.getOrElse("requestId", "").asInstanceOf[String]
    val featureName = data.eventContext.getOrElse("featureName", "").asInstanceOf[String]
    
    try {
      logger.info(s"Feature: ${featureName} | Question publishing started for : ${data.identifier} | requestId : ${requestId}")
      metrics.incCounter(config.questionPublishEventCount)
      
      val definition: ObjectDefinition = definitionCache.getDefinition(data.objectType, data.schemaVersion, config.definitionBasePath)
      val readerConfig = ExtDataConfig(config.questionKeyspaceName, definition.getExternalTable, definition.getExternalPrimaryKey, definition.getExternalProps)
      val objData = getObject(data.identifier, data.pkgVersion, data.mimeType, data.publishType, readerConfig)(neo4JUtil, cassandraUtil, config)
      val obj = if (StringUtils.isNotBlank(data.lastPublishedBy)) {
        val newMeta = objData.metadata ++ Map("lastPublishedBy" -> data.lastPublishedBy)
        new ObjectData(objData.identifier, newMeta, objData.extData, objData.hierarchy)
      } else objData
      
      val messages: List[String] = validate(obj, obj.identifier, validateQuestion)
      if (messages.isEmpty) {
        cache.del(obj.identifier)
        val enrichedObj = enrichObject(obj)(neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig)
        val objWithEcar = getObjectWithEcar(enrichedObj, pkgTypes)(ec, neo4JUtil, cloudStorageUtil, config, definitionCache, definitionConfig, httpUtil)
        logger.info(s"Feature: ${featureName} | Ecar generation done for Question: ${objWithEcar.identifier} | requestId: ${requestId}")
        saveOnSuccess(objWithEcar)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig, config)
        metrics.incCounter(config.questionPublishSuccessEventCount)
        logger.info(LoggerUtil.getExitLogs(config.jobName, requestId, s"Feature: ${featureName} | Question publishing completed successfully for : ${data.identifier}"))
      } else {
        saveOnFailure(obj, messages, data.pkgVersion)(neo4JUtil)
        metrics.incCounter(config.questionPublishFailedEventCount)
        logger.info(LoggerUtil.getExitLogs(config.jobName, requestId, s"Feature: ${featureName} | Question publishing failed for : ${data.identifier} because of data validation failed. | Errors: ${messages.mkString(", ")}"))
      }
    } catch {
      case e: Throwable => {
        val errCode = "ERR_CONTENT_QUESTION_PUBLISH_FAILED"
        val errorDesc = s"SYSTEM_ERROR: ${e.getMessage}"
        val stackTrace: String = e.getStackTraceString
        logger.error(LoggerUtil.getErrorLogs(errCode, errorDesc, requestId, stackTrace))
        throw e
      }
    }
  }
}