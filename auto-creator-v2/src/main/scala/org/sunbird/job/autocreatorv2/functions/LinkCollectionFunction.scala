package org.sunbird.job.autocreatorv2.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.autocreatorv2.helpers.CollectionUpdater
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.autocreatorv2.model.ObjectParent
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.util.HttpUtil

class LinkCollectionFunction(config: AutoCreatorV2Config, httpUtil: HttpUtil)
  extends BaseProcessFunction[ObjectParent, String](config) with CollectionUpdater {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[LinkCollectionFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def metricsList(): List[String] = {
    List()
  }

  override def processElement(event: ObjectParent, context: ProcessFunction[ObjectParent, String]#Context, metrics: Metrics): Unit = {
    logger.info(s"""Waiting 5sec before adding to hierarchy for ${event.identifier}""")
    Thread.sleep(5000)
    linkCollection(event.identifier, event.parents)(config, httpUtil)
  }
}
