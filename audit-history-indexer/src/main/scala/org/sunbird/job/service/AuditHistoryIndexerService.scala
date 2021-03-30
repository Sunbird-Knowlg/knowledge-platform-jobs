package org.sunbird.job.service

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.audithistory.domain.{AuditHistoryRecord, Event}
import org.sunbird.job.task.AuditHistoryIndexerConfig
import org.sunbird.job.util.{ElasticSearchUtil, JSONUtil}

import java.io.IOException
import java.util.{Calendar, Date, TimeZone}

trait AuditHistoryIndexerService {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[AuditHistoryIndexerService])

  def processEvent(event: Event, metrics: Metrics)(implicit esUtil: ElasticSearchUtil, config: AuditHistoryIndexerConfig): Unit = {
    if (event.isValid) {
      val identifier = event.nodeUniqueId
      logger.info("Audit learning event received :: " + identifier)
      try {
        val record = getAuditHistory(event)
        val document = JSONUtil.serialize(record)
        val indexName = getIndexName(event.ets)
        esUtil.addDocumentWithIndex(document, indexName)
        logger.info("Audit record created for " + identifier)
        metrics.incCounter(config.successEventCount)
      } catch {
        case ex: IOException =>
          logger.error("Error while indexing message :: " + event.getJson + " :: ", ex)
          metrics.incCounter(config.esFailedEventCount)
          throw ex
        case ex: Exception =>
          logger.error("Error while processing message :: " + event.getJson + " :: ", ex)
          metrics.incCounter(config.failedEventCount)
      }
    }
    else logger.info("Learning event not qualified for audit")
  }

  private def getIndexName(ets: Long)(implicit config: AuditHistoryIndexerConfig):String = {
    val cal = Calendar.getInstance(TimeZone.getTimeZone("IST"))
    cal.setTime(new Date(ets))
    config.auditHistoryIndex + "_" + cal.get(Calendar.YEAR) + "_" + cal.get(Calendar.WEEK_OF_YEAR)
  }

  def getAuditHistory(transactionDataMap: Event): AuditHistoryRecord = {
    val nodeUniqueId = StringUtils.replace(transactionDataMap.nodeUniqueId, ".img", "")
    AuditHistoryRecord(nodeUniqueId, transactionDataMap.objectType, transactionDataMap.label, transactionDataMap.graphId, transactionDataMap.userId, transactionDataMap.requestId, JSONUtil.serialize(transactionDataMap.transactionData), transactionDataMap.operationType, transactionDataMap.createdOnDate)
  }

}