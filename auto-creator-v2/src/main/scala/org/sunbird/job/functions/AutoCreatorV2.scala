package org.sunbird.job.functions

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.Event
import org.sunbird.job.service.AutoCreatorV2Service
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.{BaseProcessFunction, Metrics}

class AutoCreatorV2(config: AutoCreatorV2Config)
                          (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]],
                           stringTypeInfo: TypeInformation[String])
                          extends BaseProcessFunction[Event, String](config) with AutoCreatorV2Service{

    private[this] lazy val logger = LoggerFactory.getLogger(classOf[AutoCreatorV2])

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
        if(event.isValid) {
            processEvent(event, context, metrics)(config)
        } else metrics.incCounter(config.skippedEventCount)
    }
}
