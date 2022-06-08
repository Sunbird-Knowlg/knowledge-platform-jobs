package org.sunbird.job.dialcodecontextupdater.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.dialcodecontextupdater.domain.Event
import org.sunbird.job.dialcodecontextupdater.helpers.DialcodeContextUpdater
import org.sunbird.job.dialcodecontextupdater.task.DialcodeContextUpdaterConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.ServerException
import org.sunbird.job.helper.FailedEventHelper
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util

class DialcodeContextUpdaterFunction(config: DialcodeContextUpdaterConfig, httpUtil: HttpUtil)
                                    (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]], stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with DialcodeContextUpdater  with FailedEventHelper {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[DialcodeContextUpdaterFunction])
  lazy val defCache: DefinitionCache = new DefinitionCache()

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount, config.errorEventCount)
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
    logger.info("DialcodeContextUpdaterFunction::processElement:: Processing event for dial code context update operation for event object : " + event.obj)

    try {
      logger.info("DialcodeContextUpdaterFunction::processElement:: event edata : " + event.eData)
      if (event.isValid()) {
        updateContext(config, event, httpUtil)
      } else {
        logger.info("DialcodeContextUpdaterFunction::processElement:: Event is not qualified for dial code context update for dial code : " + event.dialcode)
        metrics.incCounter(config.skippedEventCount)
      }
    } catch {
      case e: ServerException =>
        logger.error("DialcodeContextUpdaterFunction :: Message processing failed for mid : " + event.mid() + " || " + event , e)
    }
  }


}
