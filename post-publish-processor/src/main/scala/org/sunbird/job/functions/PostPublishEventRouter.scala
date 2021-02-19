package org.sunbird.job.functions

import java.lang.reflect.Type
import java.util
import com.google.gson.reflect.TypeToken
import org.apache.commons.collections.CollectionUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.postpublish.domain.Event
import org.sunbird.job.postpublish.helpers.{BatchCreation, DialHelper, ShallowCopyPublishing}
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}

import scala.collection.JavaConverters._

case class PublishMetadata(identifier: String, contentType: String, mimeType: String, pkgVersion: Int)

class PostPublishEventRouter(config: PostPublishProcessorConfig, httpUtil: HttpUtil,
                             @transient var neo4JUtil: Neo4JUtil = null,
                             @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config) with ShallowCopyPublishing with BatchCreation with DialHelper {

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

      // Process Shallow Copy Content
      processShallowCopiedContent(identifier, context)

      // Process Batch Creation
      processBatchCreation(identifier, context)

      // Process Dialcode link
      processDialcodeLink(identifier, context, event)

    } else {
      metrics.incCounter(config.skippedEventCount)
    }
    metrics.incCounter(config.totalEventsCount)
  }

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount)
  }

  private def processShallowCopiedContent(identifier: String, context: ProcessFunction[Event, String]#Context): Unit ={
    logger.info("Process Shallow Copy for content: " + identifier)
    val shallowCopied = getShallowCopiedContents(identifier)(config, httpUtil)
    logger.info("Shallow copied by this content - " + identifier + " are: " + shallowCopied.size)
    if (shallowCopied.size > 0) {
      shallowCopied.foreach(metadata => context.output(config.shallowContentPublishOutTag, metadata))
    }
  }

  private def processBatchCreation(identifier: String, context: ProcessFunction[Event, String]#Context): Unit ={
    logger.info("Process Batch Creation for content: " + identifier)
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

  }

  private def processDialcodeLink(identifier: String, context: ProcessFunction[Event, String]#Context, event: Event): Unit ={
    logger.info("Process Dialcode Link for content: " + identifier)
    val metadata = neo4JUtil.getNodeProperties(identifier)

    if(validateContentType(metadata)(config)) {
      val linkMap = new util.HashMap[String, AnyRef](event.eData.asJava)
      linkMap.put("channel", metadata.getOrDefault("channel", ""))
      linkMap.put("dialcodes", metadata.getOrDefault("dialcodes", new util.ArrayList[String] {}))
      linkMap.put("reservedDialcodes", metadata.getOrDefault("reservedDialcodes", "{}"))
      context.output(config.linkDIALCodeOutTag, linkMap)
    }
  }

}
