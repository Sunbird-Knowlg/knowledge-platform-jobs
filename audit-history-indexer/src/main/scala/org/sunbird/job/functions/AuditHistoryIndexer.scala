package org.sunbird.job.functions

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.audithistory.domain.Event
import org.sunbird.job.service.AuditHistoryIndexerService
import org.sunbird.job.task.AuditHistoryIndexerConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.util.ElasticSearchUtil

class AuditHistoryIndexer(config: AuditHistoryIndexerConfig, @transient var esUtil: ElasticSearchUtil = null)
                          (implicit mapTypeInfo: TypeInformation[util.Map[String, Any]],
                           stringTypeInfo: TypeInformation[String])
                          extends BaseProcessFunction[Event, String](config) with AuditHistoryIndexerService{

    override def metricsList(): List[String] = {
        List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.esFailedEventCount, config.skippedEventCount)
    }

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        esUtil = new ElasticSearchUtil(config.esConnectionInfo, config.auditHistoryIndex, config.auditHistoryIndexType)
    }

    override def close(): Unit = {
        esUtil.close()
        super.close()
    }

    override def processElement(event: Event,
                                context: ProcessFunction[Event, String]#Context,
                                metrics: Metrics): Unit = {
        metrics.incCounter(config.totalEventsCount)
        if(event.isValid) {
            processEvent(event, metrics)(esUtil, config)
        } else metrics.incCounter(config.skippedEventCount)
    }
}
