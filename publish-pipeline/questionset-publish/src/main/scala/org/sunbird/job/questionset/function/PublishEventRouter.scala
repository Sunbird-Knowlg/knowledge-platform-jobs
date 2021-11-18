package org.sunbird.job.questionset.function

import com.google.gson.reflect.TypeToken
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.questionset.publish.domain.{Event, PublishMetadata}
import org.sunbird.job.questionset.task.QuestionSetPublishConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type

class PublishEventRouter(config: QuestionSetPublishConfig) extends BaseProcessFunction[Event, String](config) {


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
		if (event.validEvent()) {
			logger.info("PublishEventRouter :: Event: " + event)
			event.objectType match {
				case "Question" | "QuestionImage" => {
					logger.info("PublishEventRouter :: Sending Question For Publish Having Identifier: " + event.objectId)
					context.output(config.questionPublishOutTag, PublishMetadata(event.objectId, event.objectType, event.mimeType, event.pkgVersion, event.publishType))
				}
				case "QuestionSet" | "QuestionSetImage" => {
					logger.info("PublishEventRouter :: Sending QuestionSet For Publish Having Identifier: " + event.objectId)
					context.output(config.questionSetPublishOutTag, PublishMetadata(event.objectId, event.objectType, event.mimeType, event.pkgVersion, event.publishType))
				}
				case _ => {
					metrics.incCounter(config.skippedEventCount)
					logger.info("Invalid Object Type Received For Publish.| Identifier : " + event.objectId + " , objectType : " + event.objectType)
				}
			}
		} else {
      logger.warn("Event skipped for identifier: " + event.objectId + " objectType: " + event.objectType)
			metrics.incCounter(config.skippedEventCount)
		}
	}
}
