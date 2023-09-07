package org.sunbird.job.transaction.functions

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.transaction.domain.EventCache
import org.sunbird.job.transaction.service.TransactionEventProcessorService
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig


class ObsrvMetaDataGenerator(config: TransactionEventProcessorConfig)
                             (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]],
                              stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[EventCache, String](config) with TransactionEventProcessorService {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[ObsrvMetaDataGenerator])

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
  override def processElement(eventCache: EventCache,
                              context: ProcessFunction[EventCache, String]#Context,
                              metrics: Metrics): Unit = {
    try {
      metrics.incCounter(config.totalEventsCount)
      //
      logger.info("Processing EventCache: " + eventCache.msgid)
      //
    } catch {
      case ex: Exception =>
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(ex.getMessage, Map("partition" -> eventCache.partition, "offset" -> eventCache.offset), ex)
    }
  }
}

