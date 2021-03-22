package org.sunbird.job.service

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.task.AuditHistoryIndexerConfig
import org.sunbird.job.domain.Event
import org.sunbird.job.util.JSONUtil


trait AuditHistoryIndexerService {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[AuditHistoryIndexerService])

  def processEvent(message: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics)(implicit config: AuditHistoryIndexerConfig): Unit = {
    logger.info("Event::" + message.getJson)
  }

}