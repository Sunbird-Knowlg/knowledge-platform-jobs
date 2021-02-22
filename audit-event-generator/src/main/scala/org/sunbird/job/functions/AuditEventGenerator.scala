package org.sunbird.job.functions

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.Event
import org.sunbird.job.service.AuditEventGeneratorService
import org.sunbird.job.task.AuditEventGeneratorConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

class AuditEventGenerator(config: AuditEventGeneratorConfig)
                          (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]],
                           stringTypeInfo: TypeInformation[String])
                          extends BaseProcessFunction[Event, Event](config) {

    implicit lazy val auditEventConfig: AuditEventGeneratorConfig = config
    private[this] lazy val logger = LoggerFactory.getLogger(classOf[AuditEventGenerator])
    private var auditEventService:AuditEventGeneratorService = _
    lazy val windowTimeMilSec: Long = config.windowTime * 2000

    override def metricsList(): List[String] = {
        List(config.totalEventsCount, config.skippedEventCount, config.successEventCount, config.skippedEventCount)
    }

    override def open(parameters: Configuration): Unit = {
        auditEventService = new AuditEventGeneratorService()
        super.open(parameters)
    }

    override def close(): Unit = {
        super.close()
    }

    override def processElement(event: Event,
                                context: ProcessFunction[Event, Event]#Context,
                                metrics: Metrics): Unit = {
        metrics.incCounter(config.totalEventsCount)
        if(event.isValid) {
            logger.info("valid event::"+event.mid())
            auditEventService.processEvent(event, context, metrics)
        } else metrics.incCounter(config.skippedEventCount)
    }
}
