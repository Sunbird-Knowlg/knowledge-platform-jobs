package org.sunbird.job.functions

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.util.Neo4JUtil
import org.sunbird.job.service.AuditEventGeneratorService
import org.sunbird.job.task.AuditEventGeneratorConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

class AuditEventGenerator(config: AuditEventGeneratorConfig)
                          (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]],
                           stringTypeInfo: TypeInformation[String])
                          extends BaseProcessFunction[util.Map[String, AnyRef], String](config) {

    implicit lazy val auditEventConfig: AuditEventGeneratorConfig = config
    private[this] lazy val logger = LoggerFactory.getLogger(classOf[AuditEventGenerator])
    private var auditEventService:AuditEventGeneratorService = _

    override def metricsList(): List[String] = {
        List(config.totalEventsCount, config.skippedEventCount, config.successEventCount, config.skippedEventCount)
    }

    override def open(parameters: Configuration): Unit = {
//        val neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
        auditEventService = new AuditEventGeneratorService()
        super.open(parameters)
    }

    override def close(): Unit = {
        super.close()
    }

    override def processElement(event: util.Map[String, AnyRef],
                                context: ProcessFunction[util.Map[String, AnyRef], String]#Context,
                                metrics: Metrics): Unit = {
        metrics.incCounter(config.totalEventsCount)
        if(!event.containsKey("edata")) {
            logger.info("valid event::"+event.get("mid"))
            auditEventService.processEvent(event, context, metrics)
        } else metrics.incCounter(config.skippedEventCount)
    }
}
