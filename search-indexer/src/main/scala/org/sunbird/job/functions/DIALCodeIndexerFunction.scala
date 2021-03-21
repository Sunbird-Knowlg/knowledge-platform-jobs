package org.sunbird.job.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.compositesearch.domain.Event
import org.sunbird.job.compositesearch.helpers.{DIALCodeIndexerHelper, FailedEventHelper}
import org.sunbird.job.task.SearchIndexerConfig
import org.sunbird.job.util.ElasticSearchUtil
import scala.collection.JavaConverters._

class DIALCodeIndexerFunction(config: SearchIndexerConfig,
                              @transient var elasticUtil: ElasticSearchUtil = null)
  extends BaseProcessFunction[Event, String](config) with DIALCodeIndexerHelper with FailedEventHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[DIALCodeIndexerFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    elasticUtil = new ElasticSearchUtil(config.esConnectionInfo, config.dialcodeExternalIndex, config.dialcodeExternalIndexType)
    createDialCodeIndex()(elasticUtil)
  }

  override def close(): Unit = {
    elasticUtil.close()
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    metrics.incCounter(config.dialcodeExternalEventCount)
    try {
      upsertDIALCodeDocument(event.id, event.getMap().asScala.toMap)(elasticUtil)
      metrics.incCounter(config.successDialcodeExternalEventCount)
    } catch {
      case ex: Exception =>
        logger.error(s"Error while processing message for identifier : ${event.id}. Error : ", ex)
        metrics.incCounter(config.failedDialcodeExternalEventCount)
        val failedEvent = getFailedEvent(event, ex)
        context.output(config.failedEventOutTag, failedEvent)
        throw ex
    }
  }

  override def metricsList(): List[String] = {
    List(config.successDialcodeExternalEventCount, config.failedDialcodeExternalEventCount, config.dialcodeExternalEventCount)
  }
}