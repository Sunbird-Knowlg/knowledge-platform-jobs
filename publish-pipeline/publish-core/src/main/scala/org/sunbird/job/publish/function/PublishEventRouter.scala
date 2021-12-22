package org.sunbird.job.publish.function

import com.google.gson.reflect.TypeToken
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.publish.core.{Event, PublishMetadata}
import org.sunbird.job.publish.task.PublishCoreConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type

class PublishEventRouter (config: PublishCoreConfig)  extends BaseProcessFunction[Event, String](config) {


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
    if(event.validPublishChainEvent()) {
      context.output(config.publishChainOutTag, PublishMetadata(event.objectIdentifier, event.pkgVersion, event.publishType,event.eData,event.context,event.obj,event.publishChain))
    }
    else {
      logger.warn("Event skipped for identifier: " + event.objectIdentifier)
      metrics.incCounter(config.skippedEventCount)
    }
  }
}
