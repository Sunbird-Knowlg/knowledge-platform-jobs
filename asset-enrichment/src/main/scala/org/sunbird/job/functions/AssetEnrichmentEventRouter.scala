package org.sunbird.job.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.Event
import org.sunbird.job.task.AssetEnrichmentConfig
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
    if (validateObject(event, config.maxIterationCount)) {
      metrics.incCounter(config.totalEventsCount)
      if (event.objectType.equalsIgnoreCase("asset")) {
        event.mediaType.toLowerCase match {
          case "image" =>
            context.output(config.imageEnrichmentDataOutTag, event)
          case "video" =>
            context.output(config.videoEnrichmentDataOutTag, event)
          case _ =>
            logSkippedEvent(s"Media Type UNKNOWN. Identifier: ${event.id} & mediaType: ${event.mediaType}.")(metrics)
        }
      } else logSkippedEvent(s"ObjectType: ${event.objectType} is not asset. Skipping Event for identifier : ${event.id}.")(metrics)
    } else logSkippedEvent(s"Asset exceeded the max Iteration limit for identifier : ${event.id}.")(metrics)
  }

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.skippedEventCount)
  }

  private def validateObject(event: Event, maxIterationCount: Int): Boolean = {
    val iteration = event.eData.getOrElse("iteration", 0).asInstanceOf[Int]
    if (event.id.isEmpty) false
    else if (!event.objectType.equalsIgnoreCase("asset")) false
    else if (!event.mediaType.equalsIgnoreCase("image") && !event.mediaType.equalsIgnoreCase("video")) false
    else if (iteration == 1 && event.status.equalsIgnoreCase("processing")) true
    else if (iteration > 1 && iteration <= maxIterationCount && event.status.equalsIgnoreCase("failed")) true
    else false
  }

  def logSkippedEvent(message: String)(metrics: Metrics): Unit = {
    logger.info(message)
    metrics.incCounter(config.skippedEventCount)
  }
}