package org.sunbird.job.assetenricment.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.assetenricment.domain.Event
import org.sunbird.job.assetenricment.task.AssetEnrichmentConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

class AssetEnrichmentEventRouter(config: AssetEnrichmentConfig)
  extends BaseProcessFunction[Event, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[AssetEnrichmentEventRouter])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    logger.info(s"Processing event for AssetEnrichment for identifier : ${event.id}")
    metrics.incCounter(config.totalEventsCount)
    val message = event.validate(config.maxIterationCount)
    if (message.isEmpty) {
      event.mediaType.toLowerCase match {
        case "image" =>
          context.output(config.imageEnrichmentDataOutTag, event)
        case "video" =>
          context.output(config.videoEnrichmentDataOutTag, event)
        case _ =>
          logSkippedEvent(s"Media Type UNKNOWN. Identifier: ${event.id} & mediaType: ${event.mediaType}.")(metrics)
      }
    } else logSkippedEvent(message)(metrics)
  }

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.skippedEventCount)
  }

  def logSkippedEvent(message: String)(metrics: Metrics): Unit = {
    logger.info(message)
    metrics.incCounter(config.skippedEventCount)
  }
}