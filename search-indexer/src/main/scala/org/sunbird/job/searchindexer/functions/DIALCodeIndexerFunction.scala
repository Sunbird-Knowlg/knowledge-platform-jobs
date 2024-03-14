package org.sunbird.job.searchindexer.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.helper.FailedEventHelper
import org.sunbird.job.searchindexer.compositesearch.domain.Event
import org.sunbird.job.searchindexer.compositesearch.helpers.DIALCodeIndexerHelper
import org.sunbird.job.searchindexer.task.SearchIndexerConfig
import org.sunbird.job.util.ElasticSearchUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

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

  @throws(classOf[InvalidEventException])
  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    metrics.incCounter(config.dialcodeExternalEventCount)
    try {
      upsertDIALCodeDocument(event.id, event.getMap().asScala.toMap)(elasticUtil)
      metrics.incCounter(config.successDialcodeExternalEventCount)
    } catch {
      case ex: Throwable =>
        logger.error(s"Error while processing message for identifier : ${event.id}. Error : ", ex)
        metrics.incCounter(config.failedDialcodeExternalEventCount)
        val failedEvent = getFailedEvent(event.jobName, event.getMap(), ex)
        context.output(config.failedEventOutTag, failedEvent)
        throw new InvalidEventException(ex.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), ex)
    }
  }

  override def metricsList(): List[String] = {
    List(config.successDialcodeExternalEventCount, config.failedDialcodeExternalEventCount, config.dialcodeExternalEventCount)
  }
}