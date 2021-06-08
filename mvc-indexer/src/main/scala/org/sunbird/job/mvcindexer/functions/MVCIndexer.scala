package org.sunbird.job.mvcindexer.functions

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.mvcindexer.service.MVCIndexerService
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.util.{CassandraUtil, ElasticSearchUtil}

class MVCIndexer(config: MVCIndexerConfig, var esUtil: ElasticSearchUtil)
                          (implicit mapTypeInfo: TypeInformation[util.Map[String, Any]],
                           stringTypeInfo: TypeInformation[String])
                          extends BaseProcessFunction[Event, String](config) with MVCIndexerService{

    var cassandraUtil: CassandraUtil = _

    override def metricsList(): List[String] = {
        List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.esFailedEventCount, config.skippedEventCount, config.csFailedEventCount, config.contentApiFailedEventCount)
    }

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        if (esUtil == null) {
            esUtil = new ElasticSearchUtil(config.esConnectionInfo, config.mvcProcessorIndex, config.mvcProcessorIndexType)
        }
        var cassandraUtil: CassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
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
            processMessage(event, metrics, context)(esUtil, config)
        } else metrics.incCounter(config.skippedEventCount)
    }
}
