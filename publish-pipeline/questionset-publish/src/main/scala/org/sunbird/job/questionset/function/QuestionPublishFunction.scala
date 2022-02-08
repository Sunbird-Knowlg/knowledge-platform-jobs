package org.sunbird.job.questionset.function

import akka.dispatch.ExecutionContexts
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.questionset.publish.domain.PublishMetadata
import org.sunbird.job.questionset.publish.helpers.QuestionPublisher
import org.sunbird.job.questionset.task.QuestionSetPublishConfig
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, Neo4JUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type
import scala.concurrent.ExecutionContext

class QuestionPublishFunction(config: QuestionSetPublishConfig, httpUtil: HttpUtil,
                              @transient var neo4JUtil: Neo4JUtil = null,
                              @transient var cassandraUtil: CassandraUtil = null,
                              @transient var cloudStorageUtil: CloudStorageUtil = null,
                              @transient var definitionCache: DefinitionCache = null,
                              @transient var definitionConfig: DefinitionConfig = null)
                             (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[PublishMetadata, String](config) with QuestionPublisher {

  private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPublishFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  private val readerConfig = ExtDataConfig(config.questionKeyspaceName, config.questionTableName)

  @transient var ec: ExecutionContext = _
  private val pkgTypes = List(EcarPackageType.FULL.toString, EcarPackageType.ONLINE.toString)

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
    List(config.questionPublishEventCount, config.questionPublishSuccessEventCount, config.questionPublishFailedEventCount)
  }

  override def processElement(data: PublishMetadata, context: ProcessFunction[PublishMetadata, String]#Context, metrics: Metrics): Unit = {
    logger.info("Question publishing started for : " + data.identifier)
    metrics.incCounter(config.questionPublishEventCount)
    val obj = getObject(data.identifier, data.pkgVersion, data.mimeType, data.publishType, readerConfig)(neo4JUtil, cassandraUtil)
    val messages: List[String] = validate(obj, obj.identifier, validateQuestion)
    if (messages.isEmpty) {
      val enrichedObj = enrichObject(obj)(neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig)
      val objWithEcar = getObjectWithEcar(enrichedObj, pkgTypes)(ec, neo4JUtil, cloudStorageUtil, config, definitionCache, definitionConfig, httpUtil)
      logger.info("Ecar generation done for Question: " + objWithEcar.identifier)
      saveOnSuccess(objWithEcar)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)
      metrics.incCounter(config.questionPublishSuccessEventCount)
      logger.info("Question publishing completed successfully for : " + data.identifier)
    } else {
      saveOnFailure(obj, messages, data.pkgVersion)(neo4JUtil)
      metrics.incCounter(config.questionPublishFailedEventCount)
      logger.info("Question publishing failed for : " + data.identifier)
    }
  }
}
