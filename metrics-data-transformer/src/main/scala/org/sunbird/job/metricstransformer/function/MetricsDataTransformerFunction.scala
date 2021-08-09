package org.sunbird.job.metricstransformer.function

import java.util

import org.slf4j.LoggerFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.metricstransformer.domain.Event
import org.sunbird.job.metricstransformer.service.MetricsDataTransformerService
import org.sunbird.job.metricstransformer.task.MetricsDataTransformerConfig
import org.sunbird.job.util.{ElasticSearchUtil, HttpUtil}

class MetricsDataTransformerFunction(config: MetricsDataTransformerConfig, httpUtil: HttpUtil)
                            (implicit mapTypeInfo: TypeInformation[util.Map[String, Any]],
                             stringTypeInfo: TypeInformation[String])
                            extends BaseProcessFunction[Event, String](config) with MetricsDataTransformerService {

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventsCount)
    val propertyMap = event.transactionData("properties").asInstanceOf[Map[String, AnyRef]]
    val mapKeys = propertyMap.keySet.toList

    val eventMetrics = config.metrics.stream().toArray.map(_.asInstanceOf[String])
    val filteredKeys = eventMetrics.intersect(mapKeys)
    logger.info("Event keys ::" + filteredKeys)

    if(filteredKeys.length > 0) {
      processEvent(event, metrics, filteredKeys)(config, httpUtil)
    } else metrics.incCounter(config.skippedEventCount)

  }

}
