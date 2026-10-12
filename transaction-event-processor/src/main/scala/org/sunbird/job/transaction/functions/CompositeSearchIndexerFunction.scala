package org.sunbird.job.transaction.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.helper.FailedEventHelper
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.compositesearch.helpers.{CompositeSearchIndexerHelper, StaleEventAction, VersionCheck}
import org.sunbird.job.transaction.models.CompositeIndexer
import org.sunbird.job.transaction.task.TransactionEventProcessorConfig
import org.sunbird.job.util.{ElasticSearchUtil, JanusGraphUtil}
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}

class CompositeSearchIndexerFunction(
    config: TransactionEventProcessorConfig,
    @transient var elasticUtil: ElasticSearchUtil = null,
    @transient var janusGraphUtil: JanusGraphUtil = null
) extends BaseProcessKeyedFunction[String, Event, String](config)
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
      context: KeyedProcessFunction[String, Event, String]#Context,
      metrics: Metrics
  ): Unit = {
    metrics.incCounter(config.compositeSearchEventCount)
    try {
      val identifier = event.nodeUniqueId
      val operationType = event.operationType
      val eventLastUpdatedOn = extractLastUpdatedOn(event)

      // set when the indexed document was read to judge the event; reused by the upsert
      var indexedRead: Option[scala.collection.mutable.Map[String, AnyRef]] = None
      val cachedLastUpdatedOn =
        if (lastUpdatedCache.containsKey(identifier)) Some(lastUpdatedCache.get(identifier).longValue) else None

      val check = cacheCheck(operationType, eventLastUpdatedOn, cachedLastUpdatedOn).getOrElse {
        // cache miss: the indexed document is the baseline
        val indexed = readIndexedDocument(identifier)(elasticUtil)
        indexedRead = Some(indexed.getOrElse(scala.collection.mutable.Map[String, AnyRef]()))
        indexed.flatMap(indexedLastUpdatedOn).foreach(ts => lastUpdatedCache.put(identifier, ts))
        versionCheck(operationType, eventLastUpdatedOn, indexed)
      }

      val compositeObject: Option[CompositeIndexer] = check match {
        case VersionCheck.Apply =>
          Some(getCompositeIndexerObject(event)(config))
        case VersionCheck.Skip =>
          logger.info(s"Skipping $operationType for identifier: $identifier. Nothing to apply it to, or a newer document is already indexed.")
          None
        case VersionCheck.CheckGraph =>
          logger.debug(s"Stale event or no indexed document for identifier: $identifier. Checking JanusGraph.")
          val lookup = janusGraphUtil.lookupNodeProperties(identifier)
          staleEventAction(operationType, lookup) match {
            case StaleEventAction.RebuildFromGraph =>
              val graphProperties = lookup.get.get
              val fetchedTs = extractLastUpdatedOnFromGraph(graphProperties)
              val currentCached = if (lastUpdatedCache.containsKey(identifier)) lastUpdatedCache.get(identifier).longValue else 0L
              fetchedTs.foreach { ts => if (ts > currentCached) lastUpdatedCache.put(identifier, ts) }
              Some(buildCompositeIndexerFromGraph(identifier, graphProperties, event)(config))
            case StaleEventAction.Skip =>
              logger.info(s"Skipping stale $operationType for identifier: $identifier. Node not found in JanusGraph, it was deleted after this event.")
              None
            case StaleEventAction.ProcessOriginal =>
              logger.warn(s"Could not use JanusGraph for stale event of identifier: $identifier (${lookup.fold(_.toString, _ => "node not found")}). Processing original event.")
              Some(getCompositeIndexerObject(event)(config))
          }
      }

      compositeObject.foreach(processESMessage(_, indexedRead)(elasticUtil, defCache))

      // advance the cache for an applied event only after it has been written
      if (check == VersionCheck.Apply) {
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

  override def metricsList(): List[String] = {
    List(
      config.successCompositeSearchEventCount,
      config.failedCompositeSearchEventCount,
      config.compositeSearchEventCount
    )
  }

}
