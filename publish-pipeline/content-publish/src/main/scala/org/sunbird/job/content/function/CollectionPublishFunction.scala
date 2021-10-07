package org.sunbird.job.content.function

import akka.dispatch.ExecutionContexts
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.content.publish.domain.PublishMetadata
import org.sunbird.job.content.publish.helpers.CollectionPublisher
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, ElasticSearchUtil, HttpUtil, Neo4JUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type
import scala.concurrent.ExecutionContext

class CollectionPublishFunction(config: ContentPublishConfig, httpUtil: HttpUtil,
                             @transient var neo4JUtil: Neo4JUtil = null,
                             @transient var cassandraUtil: CassandraUtil = null,
                                @transient var esUtil: ElasticSearchUtil = null,
                             @transient var cloudStorageUtil: CloudStorageUtil = null,
                             @transient var definitionCache: DefinitionCache = null,
                             @transient var definitionConfig: DefinitionConfig = null)
                            (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[PublishMetadata, String](config) with CollectionPublisher {

  private[this] val logger = LoggerFactory.getLogger(classOf[CollectionPublishFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  private var cache: DataCache = _
  private val readerConfig = ExtDataConfig(config.hierarchyKeyspaceName, config.hierarchyTableName)

  @transient var ec: ExecutionContext = _
  private val pkgTypes = List(EcarPackageType.FULL.toString, EcarPackageType.SPINE.toString)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
    esUtil = new ElasticSearchUtil(config.esConnectionInfo, config.compositeSearchIndexName, config.compositeSearchIndexType)
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
    List(config.collectionPublishEventCount, config.collectionPublishSuccessEventCount, config.collectionPublishFailedEventCount, config.skippedEventCount)
  }

  override def processElement(data: PublishMetadata, context: ProcessFunction[PublishMetadata, String]#Context, metrics: Metrics): Unit = {
    try {
      logger.info("Collection publishing started for : " + data.identifier)
      metrics.incCounter(config.collectionPublishEventCount)
      val obj: ObjectData = getObject(data.identifier, data.pkgVersion, data.mimeType, data.publishType, readerConfig)(neo4JUtil, cassandraUtil)
      val messages: List[String] = validate(obj, obj.identifier, validateMetadata)
      if (obj.pkgVersion > data.pkgVersion) {
        metrics.incCounter(config.skippedEventCount)
        logger.info(s"""pkgVersion should be greater than or equal to the obj.pkgVersion for : ${obj.identifier}""")
      } else {
        if (messages.isEmpty) {
          // Pre-publish update
          updateProcessingNode(new ObjectData(obj.identifier, obj.metadata ++ Map("lastPublishedBy" -> data.lastPublishedBy), obj.extData, obj.hierarchy))(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)

          val isCollectionShallowCopy =  isContentShallowCopy(obj)
          val updatedObj = if (isCollectionShallowCopy) updateOriginPkgVersion(obj)(neo4JUtil) else obj

          // Clear redis cache
          cache.del(data.identifier)

          // Collection - add step to remove units of already Live content from redis - line 243
          val unitNodes = if (data.pkgVersion > 0) {
            val childNodes = getUnitsFromLiveContent(updatedObj)(neo4JUtil)
            childNodes.foreach(childId => cache.del(childId))
            childNodes
          } else null

          val enrichedObj = enrichObject(updatedObj)(neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig)
          val objWithEcar = getObjectWithEcar(enrichedObj, pkgTypes)(ec, cloudStorageUtil, config, definitionCache, definitionConfig, httpUtil)
          logger.info("Ecar generation done for Collection: " + objWithEcar.identifier)
          saveOnSuccess(objWithEcar)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)

          val children = obj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
          // Collection - update and publish children - line 418
          updateHierarchyMetadata(children, objWithEcar)(config)
          publishHierarchy(children, objWithEcar, readerConfig)(cassandraUtil)
//          if (!isContentShallowCopy) syncNodes(children, unitNodes)(esUtil)

          metrics.incCounter(config.collectionPublishSuccessEventCount)
          logger.info("Collection publishing completed successfully for : " + data.identifier)
        } else {
          saveOnFailure(obj, messages)(neo4JUtil)
          metrics.incCounter(config.collectionPublishFailedEventCount)
          logger.info("Collection publishing failed for : " + data.identifier)
        }
      }
    } catch {
      case exp:Exception =>
        exp.printStackTrace()
        logger.info("CollectionPublishFunction::processElement::Exception" + exp.getMessage)
        throw exp
    }
  }

}
