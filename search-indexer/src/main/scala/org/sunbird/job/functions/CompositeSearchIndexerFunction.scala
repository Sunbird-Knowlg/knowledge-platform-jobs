package org.sunbird.job.functions

import org.slf4j.LoggerFactory
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.SearchIndexerConfig
import org.sunbird.job.util.ElasticSearchUtil
import org.sunbird.job.compositesearch.domain.Event
import org.sunbird.job.compositesearch.helpers.{CompositeSearchIndexerHelper, FailedEventHelper}
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.models.CompositeIndexer


class CompositeSearchIndexerFunction(config: SearchIndexerConfig,
                                     @transient var elasticUtil: ElasticSearchUtil = null)
  extends BaseProcessFunction[Event, String](config) with CompositeSearchIndexerHelper with FailedEventHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[CompositeSearchIndexerFunction])
  lazy val defCache: DefinitionCache = new DefinitionCache()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    elasticUtil = new ElasticSearchUtil(config.esConnectionInfo, config.compositeSearchIndex, config.compositeSearchIndexType)
    createCompositeSearchIndex()(elasticUtil)
  }

  override def close(): Unit = {
    elasticUtil.close()
    super.close()
  }
  @throws(classOf[InvalidEventException])
  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    metrics.incCounter(config.compositeSearchEventCount)
    try {
      val compositeObject = getCompositeIndexerObject(event)
      processESMessage(compositeObject)(elasticUtil, defCache)
      metrics.incCounter(config.successCompositeSearchEventCount)
    } catch {
      case ex: Throwable =>
        metrics.incCounter(config.failedCompositeSearchEventCount)
        val failedEvent = getFailedEvent(event, ex)
        context.output(config.failedEventOutTag, failedEvent)
        throw new InvalidEventException(ex.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), ex)
    }
  }

  def getCompositeIndexerObject(event: Event): CompositeIndexer = {
    val objectType = event.readOrDefault("objectType", "")
    val graphId = event.readOrDefault("graphId", "")
    val uniqueId = event.readOrDefault("nodeUniqueId", "")
    val messageId = event.readOrDefault("mid", "")
    CompositeIndexer(graphId, objectType, uniqueId, messageId, event.getMap(), config)
  }

  override def metricsList(): List[String] = {
    List(config.successCompositeSearchEventCount, config.failedCompositeSearchEventCount, config.compositeSearchEventCount)
  }

}