package org.sunbird.job.mvcindexer.service

import org.slf4j.LoggerFactory
import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.mvcindexer.domain.Event
import org.elasticsearch.client.transport.NoNodeAvailableException
import org.sunbird.job.Metrics
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.mvcindexer.util.{ContentUtil, MVCCassandraIndexer, MVCESIndexer}
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

  @throws[Exception]
  def processMessage(message: Event, metrics: Metrics, context: ProcessFunction[Event, String]#Context): Unit = {
    logger.info("Indexing event into ES")
    try {
      processMessage(message)
      logger.info("Record Added/Updated into mvc index for " + message.identifier)
      metrics.incCounter(config.successEventCount)
    } catch {
//      case ex: RuntimeException => {
//        metrics.incCounter(config.runtimeFailedEventCount)
//        pushFailedEventForRetry(message, context, ex, PlatformErrorCodes.SYSTEM_ERROR.toString)
//        throw ex
//      }

      case ex: Exception => {
        ex.printStackTrace()
//        metrics.incCounter(config.failedEventCount)
//        pushFailedEventForRetry(message, context, ex, PlatformErrorCodes.PROCESSING_ERROR.toString)
        throw ex
      }
    }
  }

  @throws[Exception]
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

//  def pushFailedEventForRetry(eventMessage: Event, context: ProcessFunction[Event, String]#Context, error: Exception, errorCode: String): Unit = {
//    logger.error("Error while processing message: " + errorCode + " " + JSONUtil.serialize(eventMessage), error)
//
//    val failedEventMap = eventMessage.map
//    val errorString = ExceptionUtils.getStackTrace(error).split("\\n\\t")
//    val stackTrace =  if (errorString.length > 21)  java.util.Arrays.asList(errorString).subList(errorString.length - 21, errorString.length - 1)
//    else java.util.Arrays.asList(errorString)
//
//    failedEventMap.put("errorCode", errorCode)
//    failedEventMap.put("error", error.getMessage + " : : " + JSONUtil.serialize(stackTrace))
//    failedEventMap.put("jobName", config.jobName)
//    failedEventMap.put("failInfo", failedEventMap)
//
//    context.output(config.failedOutputTag, JSONUtil.serialize(failedEventMap))
//  }

  def closeConnection(): Unit = {
    cassandraUtil.close()
  }

}
