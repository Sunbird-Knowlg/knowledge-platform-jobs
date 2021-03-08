package org.sunbird.job.functions

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.CompositeSearchIndexerConfig
import org.sunbird.job.util.{DefinitionUtil, ElasticSearchUtil}
import org.sunbird.job.compositesearch.domain.Event
import org.sunbird.job.compositesearch.helpers.{CompositeSearchIndexerHelper, FailedEventHelper}
import org.sunbird.job.models.CompositeIndexer


class CompositeSearchIndexerFunction(config: CompositeSearchIndexerConfig,
                                     @transient var elasticUtil: ElasticSearchUtil = null)
  extends BaseProcessFunction[Event, String](config)
    with CompositeSearchIndexerHelper with FailedEventHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[CompositeSearchIndexerFunction])
  lazy val definitionUtil: DefinitionUtil = new DefinitionUtil(config.definitionCacheExpiry)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    elasticUtil = new ElasticSearchUtil(config.esConnectionInfo, config.compositeSearchIndex, config.compositeSearchIndexType)
    createCompositeSearchIndex()(elasticUtil)
  }

  override def close(): Unit = {
    elasticUtil.close()
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    logger.info(s"Indexing event for Composite Search into ES")
    metrics.incCounter(config.compositeSearchEventCount)
    try {
      val compositeObject = getCompositeIndexerobject(event)
      processESMessage(compositeObject)(elasticUtil, definitionUtil)
      metrics.incCounter(config.successEventCount)
    } catch {
      case ex: Exception =>
        logger.error("Error while processing message.", ex)
        metrics.incCounter(config.failedEventCount)
        val failedEvent = getFailedEvent(event, ex)
        context.output(config.failedEventOutTag, failedEvent)
        throw ex
    }
  }

  def getCompositeIndexerobject(event: Event): CompositeIndexer = {
    val objectType = event.readOrDefault("objectType", "")
    val graphId = event.readOrDefault("graphId", "")
    val uniqueId = event.readOrDefault("nodeUniqueId", "")
    val messageId = event.readOrDefault("mid", "")
    CompositeIndexer(graphId, objectType, uniqueId, messageId, event.getMap(), config)
  }

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.compositeSearchEventCount)
  }

}