package org.sunbird.job.searchindexer.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.helper.FailedEventHelper
import org.sunbird.job.searchindexer.compositesearch.domain.Event
import org.sunbird.job.searchindexer.compositesearch.helpers.CompositeSearchIndexerHelper
import org.sunbird.job.searchindexer.models.CompositeIndexer
import org.sunbird.job.searchindexer.task.SearchIndexerConfig
import org.sunbird.job.util.{CSPMetaUtil, ElasticSearchUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}


class CompositeSearchIndexerFunction(config: SearchIndexerConfig,
                                     @transient var elasticUtil: ElasticSearchUtil = null)
  extends BaseProcessFunction[Event, String](config) with CompositeSearchIndexerHelper with FailedEventHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[CompositeSearchIndexerFunction])
  lazy val defCache: DefinitionCache = new DefinitionCache()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    elasticUtil = new ElasticSearchUtil(config.esConnectionInfo, config.compositeSearchIndex)
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
        logger.error(s"Error while processing message for identifier : ${event.id}. Partition: ${event.partition} and Offset: ${event.offset}. Error : ", ex)
        metrics.incCounter(config.failedCompositeSearchEventCount)
        val failedEvent = getFailedEvent(event.jobName, event.getMap(), ex)
        context.output(config.failedEventOutTag, failedEvent)
        throw new InvalidEventException(ex.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), ex)
    }
  }

  def getCompositeIndexerObject(event: Event): CompositeIndexer = {
    val objectType = event.readOrDefault("objectType", "")
    val graphId = event.readOrDefault("graphId", "")
    val uniqueId = event.readOrDefault("nodeUniqueId", "")
    val messageId = event.readOrDefault("mid", "")

    val updateEvent: java.util.Map[String, Any] = if(config.isrRelativePathEnabled) {
      val json = CSPMetaUtil.updateAbsolutePath(event.getJson())(config)
      ScalaJsonUtil.deserialize[java.util.Map[String, Any]](json)
    } else event.getMap()

    CompositeIndexer(graphId, objectType, uniqueId, messageId, updateEvent, config)
  }

  override def metricsList(): List[String] = {
    List(config.successCompositeSearchEventCount, config.failedCompositeSearchEventCount, config.compositeSearchEventCount)
  }

}