package org.sunbird.job.transaction.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.service.TransactionEventProcessorService
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig
import org.sunbird.job.util.ElasticSearchUtil
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}

import java.util

class AuditHistoryIndexer(config: TransactionEventProcessorConfig, var esUtil: ElasticSearchUtil)
                         (implicit mapTypeInfo: TypeInformation[util.Map[String, Any]],
                          stringTypeInfo: TypeInformation[String])
  extends BaseProcessKeyedFunction[String, Event, String](config) with TransactionEventProcessorService {


  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.esFailedEventCount, config.skippedEventCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (esUtil == null) {
      esUtil = new ElasticSearchUtil(config.esConnectionInfo, config.auditHistoryIndex, config.auditHistoryIndexType)
    }
  }

  override def close(): Unit = {
    esUtil.close()
    super.close()
  }

  override def processElement(event: Event,
                              context: KeyedProcessFunction[String, Event, String]#Context,
                              metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventsCount)
    if (event.isValid) {
      processAuditHistoryEvent(event, metrics)(esUtil, config)
    } else metrics.incCounter(config.skippedEventCount)
  }
}
