package org.sunbird.job.mvcindexer.service

import org.slf4j.LoggerFactory
import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.mvcindexer.domain.Event
import org.elasticsearch.client.transport.NoNodeAvailableException
import org.sunbird.job.Metrics
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.mvcindexer.util.{ContentUtil, MVCCassandraIndexer, MVCESIndexer, PlatformErrorCodes}
import org.sunbird.job.util.{CassandraUtil, ElasticSearchUtil, HttpUtil, JSONUtil}


class MVCIndexerService {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[MVCIndexerService])
  private var mvcEsIndexer: MVCESIndexer = _
  private var cassandraUtil:CassandraUtil = _
  private var httpUtil: HttpUtil = _
  private var config: MVCIndexerConfig = _
  private var cassandraManager: MVCCassandraIndexer = _

  def this(config: MVCIndexerConfig, esUtil: ElasticSearchUtil, httpUtil: HttpUtil) = {
    this()
    this.cassandraUtil = new CassandraUtil(config.lmsDbHost, config.lmsDbPort)
    this.mvcEsIndexer = if (mvcEsIndexer == null) new MVCESIndexer(config, esUtil) else mvcEsIndexer
    this.mvcEsIndexer.createMVCSearchIndex()
    logger.info(config.mvcProcessorIndex + " created")
    this.config = config
    this.httpUtil = httpUtil
    this.cassandraManager = new MVCCassandraIndexer(config, cassandraUtil, httpUtil)
  }

  def processMessage(message: Event, metrics: Metrics, context: ProcessFunction[Event, String]#Context): Unit = {
    if (message.isValid) {
      logger.debug("Indexing event into ES")
      try {
        processMessage(message)
        logger.debug("Record Added/Updated into mvc index for " + message.identifier)
        metrics.incCounter(config.successEventCount)
      } catch {

        case ex: RuntimeException => {
          logger.error("Error while processing message: " + PlatformErrorCodes.SYSTEM_ERROR.toString + " " + JSONUtil.serialize(message), ex)
          metrics.incCounter(config.runtimeFailedEventCount)
          metrics.incCounter(config.failedEventCount)
//          pushEventForRetry(message, context, ex, PlatformErrorCodes.SYSTEM_ERROR.toString)
        }

        case ex: Exception => {
          logger.error("Error while processing message: " + JSONUtil.serialize(message), ex)
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
          message.eventData = ContentUtil.getContentMetaData(message.eventData, objectId, httpUtil, config)
        }
        logger.info("processMessage  ::: Calling cassandra insertion for " + objectId)
        cassandraManager.insertIntoCassandra(message, objectId)
      }
      logger.info("processMessage  ::: Calling elasticsearch insertion for " + objectId)
      mvcEsIndexer.upsertDocument(objectId, message)
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

  def closeConnection(): Unit = {
    cassandraUtil.close()
  }

}
