package org.sunbird.job.service

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.audithistory.domain.{AuditHistoryRecord, Event}
import org.sunbird.job.task.AuditHistoryIndexerConfig
import org.sunbird.job.util.{ElasticSearchUtil, JSONUtil}

import java.io.IOException
import java.util.{Calendar, Date, TimeZone}
import scala.collection.mutable.Map
import scala.collection.immutable

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
    val summary = setSummaryData(transactionDataMap)
    AuditHistoryRecord(nodeUniqueId, transactionDataMap.objectType, transactionDataMap.label, transactionDataMap.graphId, transactionDataMap.userId, transactionDataMap.requestId, JSONUtil.serialize(transactionDataMap.transactionData), transactionDataMap.operationType, transactionDataMap.createdOnDate, summary)
  }

  private def setSummaryData(transactionDataMap: Event): String = {
    var summaryData = Map[String, AnyRef]()
    var relations = Map[String, Integer]()
    var properties = Map[String, AnyRef]()
    var fields = List[String]()
    val transactionMap:immutable.Map[String, AnyRef] = transactionDataMap.transactionData

    for ((entryKey, entryVal) <- transactionMap) {
      var list:List[AnyRef] = null

      entryKey match {
        case "addedRelations" =>
          list = entryVal.asInstanceOf[List[AnyRef]]
          if (null != list && list.nonEmpty) relations ++= Map("addedRelations"-> list.size)
          else relations ++= Map("addedRelations"-> 0)
          summaryData ++= Map("relations"-> relations)

        case "removedRelations" =>
          list = entryVal.asInstanceOf[List[AnyRef]]
          if (null != list && list.nonEmpty) relations ++= Map("removedRelations"-> list.size)
          else relations ++= Map("removedRelations"-> 0)
          summaryData ++= Map("relations"-> relations)

        case "properties" =>
          if (StringUtils.isNotBlank(entryVal.toString)) {
            val propsMap = entryVal.asInstanceOf[immutable.Map[String, AnyRef]]
            val propertiesSet = propsMap.keySet
            if (null != propertiesSet) {
                fields ++= propertiesSet
            }
            else properties ++= Map("count"-> 0.asInstanceOf[AnyRef])
          }
          properties ++= Map("count"-> fields.size.asInstanceOf[AnyRef])
          properties ++= Map("fields"-> fields)
          summaryData ++= Map("properties"-> properties)

        case _ =>

      }
    }
    JSONUtil.serialize(summaryData)
  }

}