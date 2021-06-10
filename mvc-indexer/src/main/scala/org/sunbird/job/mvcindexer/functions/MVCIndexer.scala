package org.sunbird.job.mvcindexer.functions

import java.util
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.mvcindexer.service.MVCIndexerService
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.util.{CassandraUtil, ElasticSearchUtil, HttpUtil}

class MVCIndexer(config: MVCIndexerConfig, var esUtil: ElasticSearchUtil, httpUtil: HttpUtil)
                          (implicit mapTypeInfo: TypeInformation[util.Map[String, Any]],
                           stringTypeInfo: TypeInformation[String])
                          extends BaseProcessFunction[Event, String](config){

    var cassandraUtil: CassandraUtil = _
    var mvcIndexerService: MVCIndexerService = _

    override def metricsList(): List[String] = {
        List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.esFailedEventCount, config.skippedEventCount, config.csFailedEventCount, config.contentApiFailedEventCount)
    }

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        if (esUtil == null) {
            esUtil = new ElasticSearchUtil(config.esConnectionInfo, config.mvcProcessorIndex, config.mvcProcessorIndexType)
        }
        mvcIndexerService = new MVCIndexerService(config, esUtil, httpUtil)
    }

    override def close(): Unit = {
        esUtil.close()
        mvcIndexerService.closeConnection()
        super.close()
    }

    override def processElement(event: Event,
                                context: ProcessFunction[Event, String]#Context,
                                metrics: Metrics): Unit = {
        metrics.incCounter(config.totalEventsCount)
        if(event.isValid) {
            mvcIndexerService.processMessage(event, metrics, context)
        } else metrics.incCounter(config.skippedEventCount)
    }
}
