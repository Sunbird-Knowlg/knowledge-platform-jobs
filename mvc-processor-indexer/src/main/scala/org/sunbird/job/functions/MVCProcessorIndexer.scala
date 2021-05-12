package org.sunbird.job.functions

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.service.MVCProcessorIndexerService
import org.sunbird.job.task.MVCProcessorIndexerConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.util.{CassandraUtil, ElasticSearchUtil}

class MVCProcessorIndexer(config: MVCProcessorIndexerConfig, var esUtil: ElasticSearchUtil)
                          (implicit mapTypeInfo: TypeInformation[util.Map[String, Any]],
                           stringTypeInfo: TypeInformation[String])
                          extends BaseProcessFunction[Event, String](config) with MVCProcessorIndexerService{

    var cassandraUtil: CassandraUtil = _

    override def metricsList(): List[String] = {
        List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.esFailedEventCount, config.skippedEventCount, config.csFailedEventCount, config.contentApiFailedEventCount)
    }

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        if (esUtil == null) {
            esUtil = new ElasticSearchUtil(config.esConnectionInfo, config.mvcProcessorIndex, config.mvcProcessorIndexType)
        }
        var cassandraUtil: CassandraUtil = _
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
