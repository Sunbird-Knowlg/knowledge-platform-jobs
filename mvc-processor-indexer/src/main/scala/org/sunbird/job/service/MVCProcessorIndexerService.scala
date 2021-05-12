package org.sunbird.job.service

import org.slf4j.LoggerFactory
import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.mvcjobs.samza.service.util.MVCProcessorESIndexer
import org.sunbird.jobs.samza.util.FailedEventsUtil
import org.sunbird.searchindex.util.CompositeSearchConstants
import org.elasticsearch.client.transport.NoNodeAvailableException
import org.sunbird.job.Metrics
import org.sunbird.job.task.MVCProcessorIndexerConfig
import org.sunbird.job.util.{ElasticSearchUtil, ContentUtil, MVCProcessorCassandraIndexer, PlatformErrorCodes}


trait MVCProcessorIndexerService() {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[MVCProcessorIndexerService])
  private var mvcIndexer: MVCProcessorESIndexer = null

  private var systemStream: SystemStream = null

  private var cassandraManager: MVCProcessorCassandraIndexer = _

  def this(mvcIndexer: MVCProcessorESIndexer) = {
    this()
    this.mvcIndexer = mvcIndexer
  }

  override def initialize(config: MVCProcessorIndexerConfig): Unit = {
    this.config = config
    Json.loadProperties(config)
    logger.info("Service config initialized")
//    systemStream =
//      new SystemStream("kafka", config.get("output.failed.events.topic.name"))
    mvcIndexer = if (mvcIndexer == null) new MVCProcessorESIndexer() else mvcIndexer
    mvcIndexer.createMVCSearchIndex()
    logger.info(CompositeSearchConstants.MVC_SEARCH_INDEX + " created")
    cassandraManager = new MVCProcessorCassandraIndexer()
  }

  override def processMessage(message: Event, metrics: Metrics, context: ProcessFunction[Event, String]#Context)(implicit esUtil: ElasticSearchUtil, config: MVCProcessorIndexerConfig): Unit = {
    val index: AnyRef = message.get("index")
    val shouldindex: java.lang.Boolean =
      BooleanUtils.toBoolean(if (null == index) "true" else index.toString)
    val identifier: String = message
      .get("object")
      .asInstanceOf[Map[String, Any]]
      .get("id")
      .asInstanceOf[String]
    if (!BooleanUtils.isFalse(shouldindex)) {
      logger.debug("Indexing event into ES")
      try {
        processMessage(message)
        logger.debug("Record Added/Updated into mvc index for " + identifier)
        metrics.incCounter(config.successEventCount)
      } catch {

        case ex: RuntimeException => {
          logger.error("Error while processing message:", message, ex, PlatformErrorCodes.SYSTEM_ERROR.toString)
          metrics.incCounter(config.runtimeFailedEventCount)
          metrics.incCounter(config.failedEventCount)
          pushEventForRetry(message, context, ex, PlatformErrorCodes.SYSTEM_ERROR.toString)
        }

        case ex: Exception => {
          logger.error("Error while processing message:", message, ex)
          metrics.incCounter(config.failedEventCount)
          if (null != message) {
            val errorCode: String = if (ex.isInstanceOf[NoNodeAvailableException]) PlatformErrorCodes.SYSTEM_ERROR.toString else PlatformErrorCodes.PROCESSING_ERROR.toString
            pushEventForRetry(message, context, ex, errorCode)
          }
        }
      }
    } else {
      logger.info("Learning event not qualified for indexing")
    }
  }

  def processMessage(message: Event): Unit = {
    if (message != null && message.eventData != null) {
      var eventData: Map[String, AnyRef] =
        message.get("eventData").asInstanceOf[Map[String, AnyRef]]
      val action: String = eventData.get("action").toString
      val objectId: String = message.get("object").asInstanceOf[Map[String, AnyRef]].get("id").asInstanceOf[String]
      if (!action.equalsIgnoreCase("update-content-rating")) {
        if (action.equalsIgnoreCase("update-es-index")) {
          eventData = ContentUtil.getContentMetaData(eventData, objectId)
        }
        logger.info("MVCProcessorService :: processMessage  ::: Calling cassandra insertion for " + objectId)
        cassandraManager.insertIntoCassandra(eventData, objectId)
      }
      logger.info("MVCProcessorService :: processMessage  ::: Calling elasticsearch insertion for " + objectId)
      mvcIndexer.upsertDocument(objectId, eventData)
    }
  }

//  def pushEventForRetry(eventMessage: Event, context: ProcessFunction[Event, String]#Context, error: Exception, errorCode: String): Unit = {
//    val failedEventMap = eventMessage.map
//    val errorString = ExceptionUtils.getStackTrace(error).split("\\n\\t")
//    var stackTrace = null
//    if (errorString.length > 21) stackTrace = Arrays.asList(errorString).subList(errorString.length - 21, errorString.length - 1)
//    else stackTrace = Arrays.asList(errorString)
//    failedEventMap.put("errorCode", errorCode)
//    failedEventMap.put("error", error.getMessage + " : : " + stackTrace)
//    eventMessage.put("jobName", metrics.getJobName)
//    eventMessage.put("failInfo", failedEventMap)
//    collector.send(new Nothing(sysStream, eventMessage))
//    LOGGER.debug("Event sent to fail topic for job : " + metrics.getJobName)
//  }

}
