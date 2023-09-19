package org.sunbird.job.transaction.functions

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.slf4j.LoggerFactory
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.service.TransactionEventProcessorService
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.util.ElasticSearchUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

class TransactionEventRouter(config: TransactionEventProcessorConfig)
                            (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]],
                             stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with TransactionEventProcessorService {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[AuditEventGenerator])


  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount, config.emptySchemaEventCount, config.emptyPropsEventCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  @throws(classOf[InvalidEventException])
  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    try {
      metrics.incCounter(config.totalEventsCount)
      if (event.isValid) {
        logger.info("Valid event -> " + event.nodeUniqueId)
      }else metrics.incCounter(config.skippedEventCount)
    } catch {
      case ex: Exception =>
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(ex.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), ex)
    }
  }
}

