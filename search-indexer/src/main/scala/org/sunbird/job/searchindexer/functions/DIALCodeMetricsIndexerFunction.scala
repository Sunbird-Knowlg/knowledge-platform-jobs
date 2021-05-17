package org.sunbird.job.searchindexer.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.searchindexer.compositesearch.domain.Event
import org.sunbird.job.searchindexer.compositesearch.helpers.{DIALCodeMetricsIndexerHelper, FailedEventHelper}
import org.sunbird.job.searchindexer.task.SearchIndexerConfig
import org.sunbird.job.util.ElasticSearchUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._

class DIALCodeMetricsIndexerFunction(config: SearchIndexerConfig,
                                     @transient var elasticUtil: ElasticSearchUtil = null)
  extends BaseProcessFunction[Event, String](config) with DIALCodeMetricsIndexerHelper with FailedEventHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[DIALCodeMetricsIndexerFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    elasticUtil = new ElasticSearchUtil(config.esConnectionInfo, config.dialcodeMetricIndex, config.dialcodeMetricIndexType)
    createDialCodeIndex()(elasticUtil)
  }

  override def close(): Unit = {
    elasticUtil.close()
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    metrics.incCounter(config.dialcodeMetricEventCount)
    try {
      upsertDialcodeMetricDocument(event.id, event.getMap().asScala.toMap)(elasticUtil)
      metrics.incCounter(config.successDialcodeMetricEventCount)
    } catch {
      case ex: Throwable =>
        logger.error(s"Error while processing message for identifier : ${event.id}. Error : ", ex)
        metrics.incCounter(config.failedDialcodeMetricEventCount)
        val failedEvent = getFailedEvent(event, ex)
        context.output(config.failedEventOutTag, failedEvent)
        throw ex
    }
  }

  override def metricsList(): List[String] = {
    List(config.successDialcodeMetricEventCount, config.failedDialcodeMetricEventCount, config.dialcodeMetricEventCount)
  }

}