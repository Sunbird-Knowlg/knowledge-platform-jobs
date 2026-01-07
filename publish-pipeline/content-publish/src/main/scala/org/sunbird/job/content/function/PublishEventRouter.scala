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
    logger.info(s"PublishEventRouter :: Received Event For Publish Having Identifier: ${event.identifier}")
    
    // Event validation
    if (event.validEvent(config)) {
      event.objectType match {
        case "Content" | "ContentImage" => {
          logger.info(s"PublishEventRouter :: Sending Content For Publish Having Identifier: ${event.identifier}")
          context.output(config.contentPublishOutTag, event)
        }
        case "Collection" | "CollectionImage" => {
          logger.info(s"PublishEventRouter :: Sending Collection For Publish Having Identifier: ${event.identifier}")
          context.output(config.collectionPublishOutTag, event)
        }
        case "Question" | "QuestionImage" => {
          logger.info(s"PublishEventRouter :: Sending Question For Publish Having Identifier: ${event.identifier}")
          context.output(config.questionPublishOutTag, event)
        }
        case "QuestionSet" | "QuestionSetImage" => {
          logger.info(s"PublishEventRouter :: Sending QuestionSet For Publish Having Identifier: ${event.identifier}")
          context.output(config.questionSetPublishOutTag, event)
        }
        case _ => {
          metrics.incCounter(config.skippedEventCount)
          logger.info(s"PublishEventRouter :: Invalid Object Type Received For Publish. Identifier: ${event.identifier}, objectType: ${event.objectType}")
        }
      }
    } else {
      logger.info(s"PublishEventRouter :: Event skipped for identifier: ${event.identifier}, objectType: ${event.objectType}")
      metrics.incCounter(config.skippedEventCount)
    }
  }
}
