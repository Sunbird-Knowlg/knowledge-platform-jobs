package org.sunbird.job.mvcindexer.service

import org.slf4j.LoggerFactory
import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.mvcindexer.domain.Event
import org.elasticsearch.client.transport.NoNodeAvailableException
import org.sunbird.job.Metrics
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.mvcindexer.util.{ContentUtil, MVCCassandraIndexer, PlatformErrorCodes, MVCESIndexer}
import org.sunbird.job.util.{ElasticSearchUtil}


trait MVCIndexerService() {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[MVCIndexerService])
  private var mvcIndexer: MVCESIndexer = null

  private var cassandraManager: MVCCassandraIndexer = _
//
//  def this(mvcIndexer: MVCESIndexer) = {
//    mvcIndexer = mvcIndexer
//  }

  override def initialize(config: MVCIndexerConfig): Unit = {
//    this.config = config
//    Json.loadProperties(config)
    logger.info("Service config initialized")
//    systemStream =
//      new SystemStream("kafka", config.get("output.failed.events.topic.name"))
    mvcIndexer = if (mvcIndexer == null) new MVCESIndexer() else mvcIndexer
    mvcIndexer.createMVCSearchIndex()
    logger.info(config.mvcProcessorIndex + " created")
    cassandraManager = new MVCCassandraIndexer()
  }

  def processMessage(message: Event, metrics: Metrics, context: ProcessFunction[Event, String]#Context)(implicit esUtil: ElasticSearchUtil, config: MVCIndexerConfig): Unit = {
    if (message.isValid) {
      logger.debug("Indexing event into ES")
      try {
        processMessage(message)
        logger.debug("Record Added/Updated into mvc index for " + message.identifier)
        metrics.incCounter(config.successEventCount)
      } catch {

        case ex: RuntimeException => {
          logger.error("Error while processing message:", message, ex, PlatformErrorCodes.SYSTEM_ERROR.toString)
          metrics.incCounter(config.runtimeFailedEventCount)
          metrics.incCounter(config.failedEventCount)
//          pushEventForRetry(message, context, ex, PlatformErrorCodes.SYSTEM_ERROR.toString)
        }

        case ex: Exception => {
          logger.error("Error while processing message:", message, ex)
          metrics.incCounter(config.failedEventCount)
          if (null != message) {
            val errorCode: String = if (ex.isInstanceOf[NoNodeAvailableException]) PlatformErrorCodes.SYSTEM_ERROR.toString else PlatformErrorCodes.PROCESSING_ERROR.toString
//            pushEventForRetry(message, context, ex, errorCode)
          }
        }
      }
    } else {
      logger.info("Learning event not qualified for indexing")
    }
  }

  def processMessage(message: Event): Unit = {
    if (message != null && message.eventData != null) {
      val objectId: String = message.identifier
      if (!message.action.equalsIgnoreCase("update-content-rating")) {
        if (message.action.equalsIgnoreCase("update-es-index")) {
          message.eventData = ContentUtil.getContentMetaData(message.eventData, objectId)
        }
        logger.info("processMessage  ::: Calling cassandra insertion for " + objectId)
        cassandraManager.insertIntoCassandra(message, objectId)
      }
      logger.info("processMessage  ::: Calling elasticsearch insertion for " + objectId)
      mvcIndexer.upsertDocument(objectId, message)
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
