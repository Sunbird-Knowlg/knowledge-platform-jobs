package org.sunbird.job.functions

import java.lang.reflect.Type
import java.util
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.gson.reflect.TypeToken
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.postpublish.domain.Event
import org.sunbird.job.postpublish.helpers.{BatchCreation, ShallowCopyPublishing}
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}

import scala.collection.JavaConverters._

case class PublishMetadata(identifier: String, contentType: String, mimeType: String, pkgVersion: Int)

class PostPublishEventRouter(config: PostPublishProcessorConfig, httpUtil: HttpUtil,
                             @transient var neo4JUtil: Neo4JUtil = null,
                             @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config) with ShallowCopyPublishing with BatchCreation {

  private[this] val logger = LoggerFactory.getLogger(classOf[PostPublishEventRouter])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  val contentTypes = List("Course")

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    logger.info("Processed event using JobRequest-SerDe: " + event)
    if (event.validEvent()) {
      val identifier = event.collectionId

      //Check shallow copied contents and publish.
      val shallowCopied = getShallowCopiedContents(identifier)(config, httpUtil)
      logger.info("Shallow copied by this content - " + identifier + " are: " + shallowCopied.size)
      if (shallowCopied.size > 0) {
        shallowCopied.foreach(metadata => context.output(config.shallowContentPublishOutTag, metadata))
      }

      val metadata = neo4JUtil.getNodeProperties(identifier)
      // Validate and trigger batch creation.
      if (batchRequired(metadata, identifier)(config, cassandraUtil)) {
        val createdFor = metadata.get("createdFor").asInstanceOf[java.util.List[String]]
        val batchData = new util.HashMap[String, AnyRef]() {{
          put("identifier", identifier)
          put("name", metadata.get("name"))
          put("createdBy", metadata.get("createdBy"))
          if (CollectionUtils.isNotEmpty(createdFor))
            put("createdFor", new util.ArrayList[String](createdFor))
        }}
        context.output(config.batchCreateOutTag, batchData)
      }

      // Check DIAL Code exist or not and trigger create and link.
      if (!linkedDIALCodes(metadata, identifier)) {
        val linkMap = new util.HashMap[String, AnyRef](event.eData.asJava)
        linkMap.put("channel", metadata.get("channel"))
        context.output(config.linkDIALCodeOutTag, linkMap)
      }
    } else {
      metrics.incCounter(config.skippedEventCount)
    }
    metrics.incCounter(config.totalEventsCount)
  }

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount)
  }

  //TODO: Should we reserve or throw exception?
  def linkedDIALCodes(metadata: java.util.Map[String, AnyRef], identifier: String): Boolean = {
    if (MapUtils.isNotEmpty(metadata)) {
      val dialExist = metadata.containsKey("dialcodes")
      logger.info("Reserved DIAL Codes exists for " + identifier + " : " + dialExist)
      dialExist
    } else {
      throw new Exception("Metadata [reservedDIALExists] is not found for object: " + identifier)
    }
  }

  def validateDialCodeEvent(event: Event): Boolean = {
    val edata = event.eData.asInstanceOf[java.util.Map[String, AnyRef]]
    contentTypes.contains(edata.getOrDefault("contentType", "").asInstanceOf[String])
  }

}
