package org.sunbird.job.service

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.domain.Event
import org.sunbird.job.task.AuditEventGeneratorConfig
import org.sunbird.job.util.JSONUtil


class AuditEventGeneratorService(implicit config: AuditEventGeneratorConfig) {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[AuditEventGeneratorService])

  def processEvent(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    logger.info("AUDIT Event::" + JSONUtil.serialize(event))
    metrics.incCounter(config.successEventCount)
  }
}
