package org.sunbird.job.service

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.domain.Event
import org.sunbird.job.util.JSONUtil


trait AutoCreatorV2Service {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[AutoCreatorV2Service])

  def processEvent(message: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics)(implicit config: AutoCreatorV2Config): Unit = {
    logger.info("Event::" + JSONUtil.serialize(message))
  }

}