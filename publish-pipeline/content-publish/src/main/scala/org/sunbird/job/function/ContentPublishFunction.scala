package org.sunbird.job.function

import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util
import java.util.UUID
import java.util.Date

import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.common.dto.Response
import org.sunbird.common.exception.ResponseCode
import org.sunbird.content.publish.PublishContent
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.publish.domain.PublishMetadata
import org.sunbird.job.publish.helpers.ContentPublisher
import org.sunbird.job.task.ContentPublishConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil, Neo4JUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}
import org.sunbird.publish.util.CloudStorageUtil
import org.sunbird.common.Platform
import org.sunbird.telemetry.dto.TelemetryBJREvent
import org.sunbird.telemetry.logger.TelemetryManager

import scala.collection.JavaConverters._


class ContentPublishFunction(config: ContentPublishConfig, httpUtil: HttpUtil,
                             @transient var neo4JUtil: Neo4JUtil = null,
                             @transient var cassandraUtil: CassandraUtil = null,
                             @transient var cloudStorageUtil: CloudStorageUtil = null)
                            (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[PublishMetadata, String](config) with ContentPublisher{

  private[this] val logger = LoggerFactory.getLogger(classOf[ContentPublishFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  private val readerConfig = ExtDataConfig(config.contentKeyspaceName, config.contentTableName)
  private val validPublishStatus: List[String] = List("Live", "Unlisted")

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
    cloudStorageUtil = new CloudStorageUtil(config)
  }

  override def close(): Unit = {
    super.close()
    cassandraUtil.close()
  }

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.contentPublishEventCount, config.successEventCount, config.failedEventCount, config.postPublishEventGeneratorCount)
  }

  override def processElement(data: PublishMetadata, context: ProcessFunction[PublishMetadata, String]#Context, metrics: Metrics): Unit = {
    logger.info("Content publishing started for : " + data.identifier)
    metrics.incCounter(config.totalEventsCount)
    try{
      val obj = getObject(data.identifier, data.pkgVersion, readerConfig)(neo4JUtil, cassandraUtil)
      logger.info("Content data fetched for: " + obj.identifier + " ## metadata: " + obj.metadata)

      val messages:List[String] = validate(obj, obj.identifier, validateContent)
      logger.info("Content publishing validation error message: " + messages)

      if (messages.isEmpty && prePublishValidation(data, obj)) {
        prePublishUpdate(obj, data)
        val response: Response = publishContent(obj.identifier, data.publishType)

        val publishedObjectMap: util.Map[String, AnyRef] = neo4JUtil.getNodeProperties(data.identifier)
        val publishedStatus: String = publishedObjectMap.getOrDefault("status", "").asInstanceOf[String]
        if(response.getResponseCode.equals(ResponseCode.OK) && StringUtils.isNoneBlank(publishedStatus) && validPublishStatus.contains(publishedStatus)){
          logger.info("Object: " + data.identifier + " got published successfully with pkgVersion: " + publishedObjectMap.getOrDefault("pkgVersion", ""))
          metrics.incCounter(config.successEventCount)
          metrics.incCounter(config.contentPublishEventCount)
          // Push Post Publish Processor Event
          createPostPublishEvent(publishedObjectMap, context, metrics)
        }else{
          logger.debug("Object Publish failed for: " + data.identifier + " with responseCode: " + response.getResponseCode + " and object status: " + publishedStatus)
          metrics.incCounter(config.failedEventCount)
          throw new Exception("Content Publish Failure")
        }
      }else {
        logger.error("Metadata validation failed for: " + data.identifier)
        metrics.incCounter(config.failedEventCount)
        //TODO: pushEventForRetry to output.failed.events.topic.name
      }
    }catch {
      case e: Exception =>{
        logger.error("Failed to process message for objectId: " + data.identifier, e)
        metrics.incCounter(config.failedEventCount)
        //TODO: pushEventForRetry to output.failed.events.topic.name
      }
    }
  }

  private def createPostPublishEvent(publishedObjectMap: util.Map[String, AnyRef], context: ProcessFunction[PublishMetadata, String]#Context, metrics: Metrics): Unit = {
    val event = generateInstructionEventMetadata(publishedObjectMap)
    logger.info(s"Post Publish Processor Event Object : ${event}")
    context.output(config.generatePostPublishProcessorTag, s"""${event}""")
    metrics.incCounter(config.postPublishEventGeneratorCount)
  }

  private def generateInstructionEventMetadata(publishedObjectMap: util.Map[String, AnyRef]) = {
    try{
      val actor: util.Map[String, AnyRef] = Map[String, AnyRef]("id" -> "Post Publish Processor", "type" -> "System").asJava
      val context: util.Map[String, AnyRef] = Map[String, AnyRef](
        "channel" -> publishedObjectMap.get("channel"),
        "pdata" -> Map[String, AnyRef](
          "id" -> "org.sunbird.platform",
          "ver" -> "1.0").asJava,
        "env" -> {
          if (Platform.config.hasPath("cloud_storage.env"))
            ("env" -> Platform.config.getString("cloud_storage.env"))
          else
            null
        }
      ).asJava

      val objectData: util.Map[String, AnyRef] = Map[String, AnyRef]("id" -> publishedObjectMap.get("identifier"), "ver" -> publishedObjectMap.get("versionKey")).asJava
      val edata: util.Map[String, AnyRef] = Map[String, AnyRef]("action" -> "post-publish-process",
        "contentType" -> publishedObjectMap.get("contentType"),
        "status" -> publishedObjectMap.get("status"),
        "id" -> publishedObjectMap.get("identifier"),
        "identifier" -> publishedObjectMap.get("identifier"),
        "pkgVersion" -> publishedObjectMap.get("pkgVersion"),
        "mimeType" -> publishedObjectMap.get("mimeType"),
        "name" -> publishedObjectMap.get("name"),
        "createdBy" -> publishedObjectMap.get("createdBy"),
        "createdFor" -> publishedObjectMap.get("createdFor"),
        "trackable" -> publishedObjectMap.get("trackable"),
        "iteration" -> 1.asInstanceOf[Number],
        "artifactUrl" -> {
          if (publishedObjectMap.get("artifactUrl") != null)
            (publishedObjectMap.get("artifactUrl"))
          else
            null
        }
      ).asJava

      val telemetryBJREvent: TelemetryBJREvent = new TelemetryBJREvent

      telemetryBJREvent.setEid("BE_JOB_REQUEST")
      telemetryBJREvent.setEts(System.currentTimeMillis)
      telemetryBJREvent.setMid("LP." + System.currentTimeMillis + "." + UUID.randomUUID)
      telemetryBJREvent.setActor(actor)
      telemetryBJREvent.setContext(context)
      telemetryBJREvent.setObject(objectData)
      telemetryBJREvent.setEdata(edata)

      println("telemetryBJREvent.getEdata:: " + telemetryBJREvent.getEdata.toString)
      JSONUtil.serialize(telemetryBJREvent).asInstanceOf[Any]

    }catch {
      case e: Exception =>
        TelemetryManager.error("Error Generating BE_JOB_REQUEST event: " + e.getMessage, e)
    }
  }

  private def prePublishValidation(publishMetadata: PublishMetadata, node: ObjectData) = {
    val dbPkgVersion = node.metadata.getOrElse("pkgVersion", 0.asInstanceOf[Double]).asInstanceOf[Double]
    val eventPkgVersion = publishMetadata.pkgVersion
    dbPkgVersion<=eventPkgVersion
  }
  private def prePublishUpdate(objectData: ObjectData, publishMetadata: PublishMetadata): Unit ={
    val metadata: Map[String, AnyRef]= Map("lastPublishedBy" -> publishMetadata.lastPublishedBy.asInstanceOf[AnyRef],"versionKey" -> System.currentTimeMillis().toString.asInstanceOf[AnyRef],  "prevState" -> objectData.metadata.getOrElse("status", ""))
    val tempObjectData: ObjectData = new ObjectData(objectData.dbId.asInstanceOf[String], metadata, null, null)
    val metadataUpdateQuery = metaDataQuery(tempObjectData)


    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"${tempObjectData.identifier}"}) SET n.status="Processing", $metadataUpdateQuery,$auditPropsUpdateQuery;"""
    logger.info("ContentPublishFunction: prePublishUpdate: Query: " + query)

    neo4JUtil.executeQuery(query)
    logger.info("Content for Id: " + tempObjectData.identifier  + ", Pre-Publish Update Executed.")
  }

  private def auditPropsUpdateQuery() = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    val updatedOn = sdf.format(new Date())
    s"""n.lastUpdatedOn="$updatedOn",n.lastStatusChangedOn="$updatedOn""""
  }

  private def publishContent(identifer: String, publishType: String): Response ={
    val publishContent: PublishContent = new PublishContent(config.config)
    publishContent.publishContent(identifer, publishType)

  }
}



/*object test{
  def main(args: Array[String]): Unit = {
    println("Hi How Are You..")
    println("Hi How Are You.." + System.currentTimeMillis().toString.isInstanceOf[String])

    //val versionKey: Long  = (format(new Date())).toLong
    //println(versionKey)

  }

  def format(date: Date): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    if (null != date) try
      return sdf.format(date)
    catch {
      case e: Exception =>
    }
    null
  }
}*/
