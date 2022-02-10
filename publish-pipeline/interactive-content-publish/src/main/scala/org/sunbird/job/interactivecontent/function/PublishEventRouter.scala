package org.sunbird.job.interactivecontent.function

import com.google.gson.reflect.TypeToken
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.interactivecontent.publish.domain.Event
import org.sunbird.job.interactivecontent.task.InteractiveContentPublishConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type

class PublishEventRouter(config: InteractiveContentPublishConfig) extends BaseProcessFunction[Event, String](config) {


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

    if(event.validPublishChainEvent()) {
      logger.info("PublishEventRouter :: Sending Event For Publish Having Identifier: " + event.obj.getOrElse("id",""))
      context.output(config.publishChainEventOutTag, event)

    }
    else {
      logger.warn("Event skipped for identifier: " + event.obj.getOrElse("id",""))
      metrics.incCounter(config.skippedEventCount)
    }
  }
}
