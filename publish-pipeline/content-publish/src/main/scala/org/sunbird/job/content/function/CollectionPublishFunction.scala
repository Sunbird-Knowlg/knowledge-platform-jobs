package org.sunbird.job.content.function

//import akka.dispatch.ExecutionContexts
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.neo4j.driver.v1.exceptions.ClientException
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.content.publish.domain.Event
import org.sunbird.job.content.publish.helpers.CollectionPublisher
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.helper.FailedEventHelper
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type
import java.util.UUID
import java.util.concurrent.Executors
import scala.collection.mutable
import scala.concurrent.ExecutionContext

class CollectionPublishFunction(config: ContentPublishConfig, httpUtil: HttpUtil,
                                @transient var neo4JUtil: Neo4JUtil = null,
                                @transient var cassandraUtil: CassandraUtil = null,
                                @transient var esUtil: ElasticSearchUtil = null,
                                @transient var cloudStorageUtil: CloudStorageUtil = null,
                                @transient var definitionCache: DefinitionCache = null,
                                @transient var definitionConfig: DefinitionConfig = null)
                               (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with CollectionPublisher with FailedEventHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[CollectionPublishFunction])
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
    ec = ExecutionContext.global
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
    val readerConfig = ExtDataConfig(config.hierarchyKeyspaceName, config.hierarchyTableName, definition.getExternalPrimaryKey, definition.getExternalProps)
    logger.info("Collection publishing started for : " + data.identifier)
    metrics.incCounter(config.collectionPublishEventCount)
    val obj: ObjectData = getObject(data.identifier, data.pkgVersion, data.mimeType, data.publishType, readerConfig)(neo4JUtil, cassandraUtil, config)
    logger.info(s"KN-856: Step:1 - From DB Collection:  ${obj.identifier} | Hierarchy: ${obj.hierarchy}");
    try {
      if (obj.pkgVersion > data.pkgVersion) {
        metrics.incCounter(config.skippedEventCount)
        logger.info(s"""pkgVersion should be greater than or equal to the obj.pkgVersion for : ${obj.identifier}""")
      } else {
        val updObj = new ObjectData(obj.identifier, obj.metadata ++ Map("lastPublishedBy" -> data.lastPublishedBy, "dialcodes" -> obj.metadata.getOrElse("dialcodes",null)), obj.extData, obj.hierarchy)
        val messages: List[String] = List.empty[String] // validate(obj, obj.identifier, validateMetadata)
        if (messages.isEmpty) {
          // Pre-publish update
          updateProcessingNode(updObj)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)
          logger.info(s"KN-856: Step:2 - After updating processing status Collection:  ${updObj.identifier} | Hierarchy: ${updObj.hierarchy}");
          val isCollectionShallowCopy = isContentShallowCopy(updObj)
          val updatedObj = if (isCollectionShallowCopy) updateOriginPkgVersion(updObj)(neo4JUtil) else updObj
          logger.info(s"KN-856: Step:3 - After shallow copy status check and update Collection:  ${updatedObj.identifier} | isCollectionShallowCopy: $isCollectionShallowCopy | Hierarchy: ${updatedObj.hierarchy}");

          // Clear redis cache
          cache.del(data.identifier)
          cache.del(data.identifier + COLLECTION_CACHE_KEY_SUFFIX)
          cache.del(COLLECTION_CACHE_KEY_PREFIX + data.identifier)

          // Collection - add step to remove units of already Live content from redis - line 243 in PublishFinalizer
          val unitNodes = if (obj.metadata("identifier").asInstanceOf[String].endsWith(".img")) {
            val childNodes = getUnitsFromLiveContent(updatedObj)(cassandraUtil, readerConfig, config)
            childNodes.filter(rec => rec.nonEmpty).foreach(childId => cache.del(COLLECTION_CACHE_KEY_PREFIX + childId))
            childNodes.filter(rec => rec.nonEmpty)
          } else  List.empty

          logger.info("CollectionPublishFunction:: Live unitNodes: " + unitNodes)
          val enrichedObj = enrichObject(updatedObj)(neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig)
          logger.info(s"KN-856: Step:4 - After enriching the object Collection:  ${enrichedObj.identifier} | Hierarchy: ${enrichedObj.hierarchy}");
          logger.info("CollectionPublishFunction:: Collection Object Enriched: " + enrichedObj.identifier)
          val objWithEcar = getObjectWithEcar(enrichedObj, pkgTypes)(ec, neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig, httpUtil)
          logger.info("CollectionPublishFunction:: ECAR generation completed for Collection Object: " + objWithEcar.identifier)
          logger.info(s"KN-856: Step:5 - After WithEcar Collection:  ${objWithEcar.identifier} | Hierarchy: ${objWithEcar.hierarchy}");

          val collRelationalMetadata = getRelationalMetadata(obj.identifier, obj.pkgVersion-1, readerConfig)(cassandraUtil, config).getOrElse(Map.empty[String, AnyRef])

          val dialContextMap = if(config.enableDIALContextUpdate.equalsIgnoreCase("Yes")) fetchDialListForContextUpdate(obj)(neo4JUtil, cassandraUtil, readerConfig, config) else Map.empty[String, AnyRef]
          logger.info("CollectionPublishFunction:: dialContextMap: " + dialContextMap)

          saveOnSuccess(new ObjectData(objWithEcar.identifier, objWithEcar.metadata.-("children"), objWithEcar.extData, objWithEcar.hierarchy))(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig, config)
          logger.info("CollectionPublishFunction:: Published Collection Object metadata saved successfully to graph DB: " + objWithEcar.identifier)

          val variantsJsonString = ScalaJsonUtil.serialize(objWithEcar.metadata("variants"))
          val publishType = objWithEcar.getString("publish_type", "Public")
          val successObj = new ObjectData(objWithEcar.identifier, objWithEcar.metadata + ("status" -> (if (publishType.equalsIgnoreCase("Unlisted")) "Unlisted" else "Live"), "variants" -> variantsJsonString, "identifier" -> objWithEcar.identifier), objWithEcar.extData, objWithEcar.hierarchy)
          val children = successObj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
          logger.info(s"KN-856: Step:6 - After saveOnSuccess(Neo4J Save) Collection:  ${successObj.identifier} | Hierarchy: $children");
          // Collection - update and publish children - line 418 in PublishFinalizer
          val updatedChildren = updateHierarchyMetadata(children, successObj.metadata, collRelationalMetadata)(config)
          logger.info(s"KN-856: Step:7 - After updateHierarchyMetadata Collection:  ${successObj.identifier} | Hierarchy: $updatedChildren");
          logger.info("CollectionPublishFunction:: Hierarchy Metadata updated for Collection Object: " + successObj.identifier + " || updatedChildren:: " + updatedChildren)
          publishHierarchy(updatedChildren, successObj, readerConfig, config)(cassandraUtil)
          logger.info(s"KN-856: Step:8 - After publishHierarchy Collection:  ${successObj.identifier} | Hierarchy: $updatedChildren");
          //TODO: Save IMAGE Object with enrichedObj children and collRelationalMetadata when pkgVersion is 1 - verify with MaheshG
          if(data.pkgVersion == 1) {
            saveImageHierarchy(enrichedObj, readerConfig, collRelationalMetadata)(cassandraUtil)
            logger.info(s"KN-856: Step:8.1 - After saveImageHierarchy Collection:  ${enrichedObj.identifier} | Hierarchy: ${enrichedObj.hierarchy}");
          }

          if (!isCollectionShallowCopy) syncNodes(successObj, updatedChildren, unitNodes)(esUtil, neo4JUtil, cassandraUtil, readerConfig, definition, config)
          pushPostProcessEvent(successObj, dialContextMap, context)(metrics)
          logger.info(s"KN-856: Step:9 - After pushPostProcessEvent Collection:  ${successObj.identifier} | Hierarchy: ${successObj.hierarchy}");
          metrics.incCounter(config.collectionPublishSuccessEventCount)
          logger.info("CollectionPublishFunction:: Collection publishing completed successfully for : " + data.identifier)
        } else {
          saveOnFailure(obj, messages, data.pkgVersion)(neo4JUtil)
          val errorMessages = messages.mkString("; ")
          pushFailedEvent(data, errorMessages, null, context)(metrics)
          logger.info("CollectionPublishFunction:: Collection publishing failed for : " + data.identifier)
        }
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

  private def pushPostProcessEvent(obj: ObjectData, dialContextMap: Map[String, AnyRef] ,context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    try {
      val event = getPostProcessEvent(obj, dialContextMap)
      context.output(config.generatePostPublishProcessTag, event)
      metrics.incCounter(config.collectionPostPublishProcessEventCount)
    } catch  {
      case ex: Exception =>  ex.printStackTrace()
        throw new InvalidInputException("CollectionPublisher:: pushPostProcessEvent:: Error while pushing post process event.", ex)
    }
  }

  def getPostProcessEvent(obj: ObjectData, dialContextMap: Map[String, AnyRef]): String = {
    val ets = System.currentTimeMillis
    val mid = s"""LP.$ets.${UUID.randomUUID}"""
    val channelId = obj.metadata("channel")
    val ver = obj.metadata("versionKey")
    val contentType = obj.metadata("contentType")
    val status = obj.metadata("status")

    val serAddContextDialCode = ScalaJsonUtil.serialize(dialContextMap.getOrElse("addContextDialCodes", mutable.Map.empty).asInstanceOf[mutable.Map[List[String], String]].map(rec => (ScalaJsonUtil.serialize(rec._1) -> rec._2)))
    val serRemoveContextDialCode = ScalaJsonUtil.serialize(dialContextMap.getOrElse("removeContextDialCodes", mutable.Map.empty).asInstanceOf[mutable.Map[List[String], String]].map(rec => (ScalaJsonUtil.serialize(rec._1) -> rec._2)))

    //TODO: deprecate using contentType in the event.
    val event = s"""{"eid":"BE_JOB_REQUEST", "ets": $ets, "mid": "$mid", "actor": {"id": "Post Publish Processor", "type": "System"}, "context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}, "channel":"$channelId","env":"${config.jobEnv}"},"object":{"ver":"$ver","id":"${obj.identifier}"},"edata": {"action":"post-publish-process","iteration":1,"identifier":"${obj.identifier}","channel":"$channelId","mimeType":"${obj.mimeType}","contentType":"$contentType","pkgVersion":${obj.pkgVersion},"status":"$status","name":"${obj.metadata("name")}","trackable":${obj.metadata.getOrElse("trackable",ScalaJsonUtil.serialize(Map.empty))}, "addContextDialCodes": ${serAddContextDialCode}, "removeContextDialCodes": ${serRemoveContextDialCode} }}""".stripMargin
    logger.info(s"Post Publish Process Event for identifier ${obj.identifier}  is  : $event")
    event
  }

  private def pushFailedEvent(event: Event, errorMessage: String, error: Throwable, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Unit = {
    val failedEvent = if (error == null) getFailedEvent(event.jobName, event.getMap(), errorMessage) else getFailedEvent(event.jobName, event.getMap(), error)
    context.output(config.failedEventOutTag, failedEvent)
    metrics.incCounter(config.collectionPublishFailedEventCount)
  }

}
