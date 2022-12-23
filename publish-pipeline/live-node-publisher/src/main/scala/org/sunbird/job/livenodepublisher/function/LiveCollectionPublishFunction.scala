package org.sunbird.job.livenodepublisher.function

import akka.dispatch.ExecutionContexts
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.neo4j.driver.v1.exceptions.ClientException
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.exception.{InvalidInputException, ServerException}
import org.sunbird.job.helper.FailedEventHelper
import org.sunbird.job.livenodepublisher.publish.domain.Event
import org.sunbird.job.livenodepublisher.publish.helpers.LiveCollectionPublisher
import org.sunbird.job.livenodepublisher.task.LiveNodePublisherConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type
import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._

class LiveCollectionPublishFunction(config: LiveNodePublisherConfig, httpUtil: HttpUtil,
                                    @transient var neo4JUtil: Neo4JUtil = null,
                                    @transient var cassandraUtil: CassandraUtil = null,
                                    @transient var esUtil: ElasticSearchUtil = null,
                                    @transient var cloudStorageUtil: CloudStorageUtil = null,
                                    @transient var definitionCache: DefinitionCache = null,
                                    @transient var definitionConfig: DefinitionConfig = null)
                                   (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with LiveCollectionPublisher with FailedEventHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[LiveCollectionPublishFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  private var cache: DataCache = _
  private val COLLECTION_CACHE_KEY_PREFIX = "hierarchy_"
  private val COLLECTION_CACHE_KEY_SUFFIX = ":leafnodes"

  @transient var ec: ExecutionContext = _
  private val pkgTypes = List(EcarPackageType.SPINE, EcarPackageType.ONLINE)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort, config)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName, config)
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
    List(config.collectionPublishEventCount, config.collectionPublishSuccessEventCount, config.collectionPublishFailedEventCount, config.skippedEventCount, config.collectionPostPublishProcessEventCount)
  }

  override def processElement(data: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    val definition: ObjectDefinition = definitionCache.getDefinition(data.objectType, config.schemaSupportVersionMap.getOrElse(data.objectType.toLowerCase(), "1.0").asInstanceOf[String], config.definitionBasePath)
    val readerConfig = ExtDataConfig(config.hierarchyKeyspaceName, config.hierarchyTableName, definition.getExternalPrimaryKey(), definition.getExternalProps())
    logger.info("Collection publishing started for : " + data.identifier)
    metrics.incCounter(config.collectionPublishEventCount)
    val obj: ObjectData = getObject(data.identifier, data.pkgVersion, data.mimeType, data.publishType, readerConfig)(neo4JUtil, cassandraUtil, config)
    try {
      val childNodesMetadata: List[String] = obj.metadata.getOrElse("childNodes", new java.util.ArrayList()).asInstanceOf[java.util.List[String]].asScala.toList
      val addedResources: List[String] = searchContents(data.identifier, childNodesMetadata.toArray, config, httpUtil)
      val addedResourcesMigrationVersion: List[String] = searchContents(data.identifier, childNodesMetadata.toArray, config, httpUtil, true)

      val isCollectionShallowCopy = isContentShallowCopy(obj)
      val shallowCopyOriginMigrationVersion: Double = if(isCollectionShallowCopy) {
        val originId = obj.metadata.getOrElse("origin", "").asInstanceOf[String]
        val originNodeMetadata = neo4JUtil.getNodeProperties(originId)
        if (null != originNodeMetadata) {
          originNodeMetadata.getOrDefault("migrationVersion", "0").toString.toDouble
        } else 0
      } else 0

      if (obj.pkgVersion > data.pkgVersion) {
        metrics.incCounter(config.skippedEventCount)
        logger.info(s"""pkgVersion should be greater than or equal to the obj.pkgVersion for : ${obj.identifier}""")
      }
      else if(isCollectionShallowCopy && shallowCopyOriginMigrationVersion != 1)  {
        pushSkipEvent(data, "Origin node is found to be not migrated", context)(metrics)
      }
      else if(addedResources.size != addedResourcesMigrationVersion.size) {
        val errorMessageIdentifiers = addedResources.filter(rec => !addedResourcesMigrationVersion.contains(rec)).mkString(",")
        pushSkipEvent(data, "Non migrated contents found: " + errorMessageIdentifiers, context)(metrics)
      } else {
        val updObj = new ObjectData(obj.identifier, obj.metadata ++ Map("lastPublishedBy" -> data.lastPublishedBy, "dialcodes" -> obj.metadata.getOrElse("dialcodes",null)), obj.extData, obj.hierarchy)

        // Pre-publish update
        updateProcessingNode(updObj)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)

        val updatedObj = if (isCollectionShallowCopy) updateOriginPkgVersion(updObj)(neo4JUtil) else updObj

        // Clear redis cache
        cache.del(data.identifier)
        cache.del(data.identifier + COLLECTION_CACHE_KEY_SUFFIX)
        cache.del(COLLECTION_CACHE_KEY_PREFIX + data.identifier)

        val enrichedObjTemp = enrichObjectMetadata(updatedObj)(neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig)
        val enrichedObj = enrichedObjTemp.getOrElse(updatedObj)
        logger.info("CollectionPublishFunction:: Collection Object Enriched: " + enrichedObj.identifier)
        val objWithEcar = getObjectWithEcar(enrichedObj, pkgTypes)(ec, neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig, httpUtil)
        logger.info("CollectionPublishFunction:: ECAR generation completed for Collection Object: " + objWithEcar.identifier)

        val collRelationalMetadata = getRelationalMetadata(obj.identifier, readerConfig)(cassandraUtil).getOrElse(Map.empty[String, AnyRef])

        val variantsJsonString = ScalaJsonUtil.serialize(objWithEcar.metadata("variants"))
        val publishType = objWithEcar.getString("publish_type", "Public")
        val successObj = new ObjectData(objWithEcar.identifier, objWithEcar.metadata + ("status" -> (if (publishType.equalsIgnoreCase("Unlisted")) "Unlisted" else "Live"), "variants" -> variantsJsonString, "identifier" -> objWithEcar.identifier), objWithEcar.extData, objWithEcar.hierarchy)
        val children = successObj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]

        // Collection - update and publish children - line 418 in PublishFinalizer
        val updatedChildren = updateHierarchyMetadata(children, successObj.metadata, collRelationalMetadata)(config)
        logger.info("CollectionPublishFunction:: Hierarchy Metadata updated for Collection Object: " + successObj.identifier + " || updatedChildren:: " + updatedChildren)
        publishHierarchy(updatedChildren, successObj, readerConfig, config)(cassandraUtil)

        if (!isCollectionShallowCopy) syncNodes(successObj, updatedChildren, List.empty)(esUtil, neo4JUtil, cassandraUtil, readerConfig, definition, config)

        metrics.incCounter(config.collectionPublishSuccessEventCount)
        logger.info("CollectionPublishFunction:: Collection publishing completed successfully for : " + data.identifier)

        saveOnSuccess(new ObjectData(objWithEcar.identifier, objWithEcar.metadata.-("children") , objWithEcar.extData, objWithEcar.hierarchy))(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig, config)
        logger.info("CollectionPublishFunction:: Published Collection Object metadata saved successfully to graph DB: " + objWithEcar.identifier)
      }
    } catch {
      case ex@(_: InvalidInputException | _: ClientException) => // ClientException - Invalid input exception.
        ex.printStackTrace()
        saveOnFailure(obj, List(ex.getMessage), data.pkgVersion)(neo4JUtil)
        pushFailedEvent(data, null, ex, context)(metrics)
        logger.error(s"CollectionPublishFunction::Error while publishing collection :: ${data.partition} and Offset: ${data.offset}. Error : ${ex.getMessage}", ex)
      case ex: Exception =>
        ex.printStackTrace()
        saveOnFailure(obj, List(ex.getMessage), data.pkgVersion)(neo4JUtil)
        logger.error(s"CollectionPublishFunction::Error while processing message for Partition: ${data.partition} and Offset: ${data.offset}. Error : ${ex.getMessage}", ex)
        throw ex
    }
  }

  private def pushFailedEvent(event: Event, errorMessage: String, error: Throwable, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    val failedEvent = if (error == null)
      if(errorMessage.length>500) getFailedEvent(event.jobName, event.getMap(), errorMessage.substring(0,500))  else getFailedEvent(event.jobName, event.getMap(), errorMessage)
    else getFailedEvent(event.jobName, event.getMap(), error)
    context.output(config.failedEventOutTag, failedEvent)
    metrics.incCounter(config.collectionPublishFailedEventCount)
  }

  private def pushSkipEvent(event: Event, skipMessage: String, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    val skipEvent = if(skipMessage.length>500) getFailedEvent(event.jobName, event.getMap(), skipMessage.substring(0,500))  else getFailedEvent(event.jobName, event.getMap(), skipMessage)
    context.output(config.skippedEventOutTag, skipEvent)
    metrics.incCounter(config.skippedEventCount)
  }

  private def searchContents(collectionId: String, identifiers: Array[String], config: LiveNodePublisherConfig, httpUtil: HttpUtil, fetchMigratedVersion: Boolean = false): List[String] = {
    try {
      val reqMap = new java.util.HashMap[String, AnyRef]() {
        put("request", new java.util.HashMap[String, AnyRef]() {
          put("filters", new java.util.HashMap[String, AnyRef]() {
            put("visibility", "Default")
            put("identifiers", identifiers)
            if (fetchMigratedVersion) put("migratedVersion", 1.asInstanceOf[Number])
          })
          put("fields", config.searchFields)
        })
      }

      val requestUrl = s"${config.searchServiceBaseUrl}/v3/search"
      logger.info("CollectionPublishFunction :: searchContent :: Search Content requestUrl: " + requestUrl)
      logger.info("CollectionPublishFunction :: searchContent :: Search Content reqMap: " + reqMap)
      val httpResponse = httpUtil.post(requestUrl, JSONUtil.serialize(reqMap))
      if (httpResponse.status == 200) {
        val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
        val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        val contents = result.getOrElse("content", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
        val count = result.getOrElse("count", 0).asInstanceOf[Int]
        if (count > 0) {
          contents.map(content => {
            content.getOrElse("identifier", "").toString
          })
        } else {
          logger.info("ContentAutoCreator :: searchContent :: Received 0 count while searching childNodes : ")
          List.empty[String]
        }
      } else {
        throw new ServerException("ERR_API_CALL", "Invalid Response received while searching childNodes : " + getErrorDetails(httpResponse))
      }
    } catch {
      case ex: Exception => throw new InvalidInputException("Exception while searching children data for collection:: " + collectionId + " || ex: " + ex.getMessage)
    }
  }

  private def getErrorDetails(httpResponse: HTTPResponse): String = {
    val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
    if (null != response) " | Response Code :" + httpResponse.status + " | Result : " + response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]] + " | Error Message : " + response.getOrElse("params", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    else " | Null Response Received."
  }

}
