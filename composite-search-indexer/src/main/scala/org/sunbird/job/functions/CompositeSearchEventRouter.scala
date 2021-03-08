package org.sunbird.job.functions

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.BooleanUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.compositesearch.domain.Event
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.CompositeSearchIndexerConfig

class CompositeSearchEventRouter(config: CompositeSearchIndexerConfig)
  extends BaseProcessFunction[Event, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CompositeSearchEventRouter])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    logger.info("Processing event for CompositeSearch : " + event)
    metrics.incCounter(config.totalEventsCount)
    if (!BooleanUtils.isFalse(event.index) && event.operationType != null) {
      logger.info(s"Indexing event into ES")
      event.readOrDefault("nodeType", "") match {
        case "SET" | "DATA_NODE" => context.output(config.compositveSearchDataOutTag, event)
        case "EXTERNAL" => context.output(config.dialCodeExternalOutTag, event)
        case "DIALCODE_METRICS" => context.output(config.dialCodeMetricOutTag, event)
        case _ => logger.info(s"UNKNOWN EVENT NODETYPE.")
      }
    } else {
      metrics.incCounter(config.skippedEventCount)
      logger.info("Event not qualified for indexing.")
    }
  }

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.skippedEventCount)
  }
}