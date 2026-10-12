package org.sunbird.job.transaction.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.helper.FailedEventHelper
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.compositesearch.helpers.{CompositeSearchIndexerHelper, StaleEventAction}
import org.sunbird.job.transaction.models.CompositeIndexer
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig
import org.sunbird.job.util.{ElasticSearchUtil, JanusGraphUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

class CompositeSearchIndexerFunction(
    config: TransactionEventProcessorConfig,
    @transient var elasticUtil: ElasticSearchUtil = null,
    @transient var janusGraphUtil: JanusGraphUtil = null
) extends BaseProcessFunction[Event, String](config)
    with CompositeSearchIndexerHelper
    with FailedEventHelper {

  private[this] val logger =
    LoggerFactory.getLogger(classOf[CompositeSearchIndexerFunction])
  lazy val defCache: DefinitionCache = new DefinitionCache()

  private val MAX_CACHE_SIZE = 1000
  @transient private var lastUpdatedCache: java.util.LinkedHashMap[String, Long] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    elasticUtil = new ElasticSearchUtil(
      config.esConnectionInfo,
      config.compositeSearchIndex
    )
    janusGraphUtil = new JanusGraphUtil(config)
    lastUpdatedCache = new java.util.LinkedHashMap[String, Long](MAX_CACHE_SIZE, 0.75f, true) {
      override def removeEldestEntry(eldest: java.util.Map.Entry[String, Long]): Boolean =
        size() > MAX_CACHE_SIZE
    }
    createCompositeSearchIndex()(elasticUtil)
  }

  override def close(): Unit = {
    if (elasticUtil != null) {
      elasticUtil.close()
    }

    if (lastUpdatedCache != null) {
      lastUpdatedCache.clear()
    }
    super.close()
  }

  @throws(classOf[InvalidEventException])
  override def processElement(
      event: Event,
      context: ProcessFunction[Event, String]#Context,
      metrics: Metrics
  ): Unit = {
    metrics.incCounter(config.compositeSearchEventCount)
    try {
      val identifier = event.nodeUniqueId
      val eventLastUpdatedOn = extractLastUpdatedOn(event)
      val stale = isStaleEvent(identifier, eventLastUpdatedOn)

      val compositeObject: Option[CompositeIndexer] = if (stale) {
        logger.debug(s"Stale/out-of-order event for identifier: $identifier. Fetching latest data from JanusGraph.")
        val lookup = janusGraphUtil.lookupNodeProperties(identifier)
        staleEventAction(event.operationType, lookup) match {
          case StaleEventAction.RebuildFromGraph =>
            val graphProperties = lookup.get.get
            val fetchedTs = extractLastUpdatedOnFromGraph(graphProperties)
            val currentCached = Option(lastUpdatedCache.get(identifier)).map(_.longValue).getOrElse(0L)
            fetchedTs.foreach { ts => if (ts > currentCached) lastUpdatedCache.put(identifier, ts) }
            Some(buildCompositeIndexerFromGraph(identifier, graphProperties, event)(config))
          case StaleEventAction.Skip =>
            logger.info(s"Skipping stale ${event.operationType} for identifier: $identifier. Node not found in JanusGraph, it was deleted after this event.")
            None
          case StaleEventAction.ProcessOriginal =>
            logger.warn(s"Could not use JanusGraph for stale event of identifier: $identifier (${lookup.fold(_.toString, _ => "node not found")}). Processing original event.")
            Some(getCompositeIndexerObject(event)(config))
        }
      } else {
        Some(getCompositeIndexerObject(event)(config))
      }

      compositeObject.foreach(processESMessage(_)(elasticUtil, defCache))

      // Update cache for non-stale events; the stale path updates the cache above.
      if (!stale) {
        eventLastUpdatedOn.foreach(ts => lastUpdatedCache.put(identifier, ts))
      }
      metrics.incCounter(config.successCompositeSearchEventCount)
    } catch {
      case ex: Throwable =>
        logger.error(
          s"Error while processing message for identifier : ${event.id}. Partition: ${event.partition} and Offset: ${event.offset}. Error : ",
          ex
        )
        metrics.incCounter(config.failedCompositeSearchEventCount)
        val failedEvent = getFailedEvent(event.jobName, event.getMap(), ex)
        context.output(config.failedEventOutTag, failedEvent)
        throw new InvalidEventException(
          ex.getMessage,
          Map("partition" -> event.partition, "offset" -> event.offset),
          ex
        )
    }
  }

  /** Returns true when the event's lastUpdatedOn is older than or equal to
   *  the last successfully processed timestamp cached for the same node.
   */
  private def isStaleEvent(identifier: String, eventLastUpdatedOn: Option[Long]): Boolean = {
    eventLastUpdatedOn match {
      case Some(ts) =>
        val cached = lastUpdatedCache.get(identifier)
        cached != null && ts <= cached
      case None => false
    }
  }

  override def metricsList(): List[String] = {
    List(
      config.successCompositeSearchEventCount,
      config.failedCompositeSearchEventCount,
      config.compositeSearchEventCount
    )
  }

}
