package org.sunbird.job.content.function

import com.google.gson.reflect.TypeToken
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.content.publish.domain.Event
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type

class PublishEventRouter(config: ContentPublishConfig) extends BaseProcessFunction[Event, String](config) {


  private[this] val logger = LoggerFactory.getLogger(classOf[PublishEventRouter])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.skippedEventCount, config.totalEventsCount)
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventsCount)
    logger.info("PublishEventRouter :: Event: " + event)
    // Event validation
    if (event.validEvent(config)) {
      event.objectType match {
        case "Content" | "ContentImage" => {
          logger.info("PublishEventRouter :: Sending Content For Publish Having Identifier: " + event.identifier)
          context.output(config.contentPublishOutTag, event)
        }
        case "Collection" | "CollectionImage" => {
          logger.info("PublishEventRouter :: Sending Collection For Publish Having Identifier: " + event.identifier)
          context.output(config.collectionPublishOutTag, event)
        }
        case _ => {
          metrics.incCounter(config.skippedEventCount)
          logger.info("Invalid Object Type Received For Publish.| Identifier : " + event.identifier + " , objectType : " + event.objectType)
        }
      }
    } else {
      logger.warn("Event skipped for identifier: " + event.identifier + " objectType: " + event.objectType)
      metrics.incCounter(config.skippedEventCount)
    }
  }
}
