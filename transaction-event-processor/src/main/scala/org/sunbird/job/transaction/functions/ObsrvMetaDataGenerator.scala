package org.sunbird.job.transaction.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.transaction.domain.{Event, ObsrvEvent}
import org.sunbird.job.transaction.service.TransactionEventProcessorService
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig

import java.util

class ObsrvMetaDataGenerator(config: TransactionEventProcessorConfig)
  extends BaseProcessFunction[Event, String](config) with TransactionEventProcessorService {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[ObsrvMetaDataGenerator])

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.esFailedEventCount, config.skippedEventCount,
      config.totalObsrvMetaDataGeneratorEventsCount, config.skippedObsrvMetaDataGeneratorEventsCount, config.failedObsrvMetaDataGeneratorEventsCount, config.obsrvMetaDataGeneratorEventsSuccessCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  @throws(classOf[InvalidEventException])
  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    try {
      metrics.incCounter(config.totalObsrvMetaDataGeneratorEventsCount)
      if (event.isValid) {
        logger.info("Valid obsrv metadata generator event: " + event.nodeUniqueId)
        processEvent(event, context, metrics)(config)
      } else metrics.incCounter(config.skippedObsrvMetaDataGeneratorEventsCount)
    } catch {
      case ex: Exception =>
        metrics.incCounter(config.failedObsrvMetaDataGeneratorEventsCount)
        throw new InvalidEventException(ex.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), ex)
    }
  }
}


