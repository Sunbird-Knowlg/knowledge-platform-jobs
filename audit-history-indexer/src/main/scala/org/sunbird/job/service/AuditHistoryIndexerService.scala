package org.sunbird.job.service

import org.apache.commons.lang3.BooleanUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.task.AuditHistoryIndexerConfig
import org.sunbird.job.domain.{Event, AuditHistoryRecord}
import org.sunbird.job.util.{ElasticSearchUtil, JSONUtil}


trait AuditHistoryIndexerService {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[AuditHistoryIndexerService])

  def processEvent(event: Event, metrics: Metrics)(implicit esUtil: ElasticSearchUtil): Unit = {
    if (event.isValid) {
      logger.debug("Audit learning event received")
      try {
        val record = getAuditHistory(event)
        val identifier = message.get("nodeUniqueId").asInstanceOf[Nothing]
        logger.info("Audit record created for " + identifier)
        val entity_map = mapper.convertValue(record, classOf[Nothing])
        val document = mapper.writeValueAsString(entity_map)
        LOGGER.debug("Saving the record into ES")
        val indexName = getIndexName(String.valueOf(message.get("ets")))
        ElasticSearchUtil.addDocument(indexName, AuditHistoryConstants.AUDIT_HISTORY_INDEX_TYPE, document)
        metrics.incSuccessCounter
      } catch {
        case ex: Nothing =>
          logger.error("Error while processing message", message, ex)
          metrics.incErrorCounter
      }
    }
    else logger.info("Learning event not qualified for audit")
  }

  private def getAuditHistory(transactionDataMap: Event):AuditHistoryRecord = {
    val record: AuditHistoryRecord = new AuditHistoryRecord();
    record.userId = transactionDataMap.userId
    record.setRequestId(transactionDataMap.get("requestId").asInstanceOf[Nothing])
    var nodeUniqueId = transactionDataMap.nodeUniqueId
    if (StringUtils.endsWith(nodeUniqueId, ".img")) {
      nodeUniqueId = StringUtils.replace(nodeUniqueId, ".img", "")
      record.setObjectId(nodeUniqueId)
    }
    record.setObjectId(nodeUniqueId)
    val summary = setSummaryData(transactionDataMap)
    record.setSummary(summary)
    val createdOn = transactionDataMap.get("createdOn").asInstanceOf[Nothing]
    var date = new Nothing
    if (StringUtils.isNotBlank(createdOn)) date = df.parse(createdOn)
    record.setCreatedOn(date)
    new AuditHistoryRecord(nodeUniqueId, transactionDataMap.objectType, transactionDataMap.label, transactionDataMap.graphId, transactionDataMap.userId, transactionDataMap.requestId, JSONUtil.serialize(transactionDataMap.transactionData), transactionDataMap.operationType, transactionDataMap.createdOn, summary)
    record
  }

  private def setSummaryData(transactionDataMap: Nothing) = {
    val summaryData = new Nothing
    val relations = new Nothing
    val tags = new Nothing
    val properties = new Nothing
    val fields = new Nothing
    var transactionMap = null
    var summaryResult = null
    transactionMap = transactionDataMap.get("transactionData").asInstanceOf[Nothing]
    import scala.collection.JavaConversions._
    for (entry <- transactionMap.entrySet) {
      var list = null
      entry.getKey match {
        case "addedRelations" =>
          list = entry.getValue.asInstanceOf[Nothing]
          if (null != list && !list.isEmpty) relations.put("addedRelations", list.size)
          else relations.put("addedRelations", 0)
          summaryData.put("relations", relations)

        case "removedRelations" =>
          list = entry.getValue.asInstanceOf[Nothing]
          if (null != list && !list.isEmpty) relations.put("removedRelations", list.size)
          else relations.put("removedRelations", 0)
          summaryData.put("relations", relations)

        case "addedTags" =>
          list = entry.getValue.asInstanceOf[Nothing]
          if (null != list && !list.isEmpty) tags.put("addedTags", list.size)
          else tags.put("addedTags", 0)
          summaryData.put("tags", tags)

        case "removedTags" =>
          list = entry.getValue.asInstanceOf[Nothing]
          if (null != list && !list.isEmpty) tags.put("removedTags", list.size)
          else tags.put("removedTags", 0)
          summaryData.put("tags", tags)

        case "properties" =>
          if (StringUtils.isNotBlank(entry.getValue.toString)) {
            val propsMap = entry.getValue.asInstanceOf[Nothing]
            val propertiesSet = propsMap.keySet
            if (null != propertiesSet) {
              import scala.collection.JavaConversions._
              for (s <- propertiesSet) {
                fields.add(s)
              }
            }
            else properties.put("count", 0)
          }
          properties.put("count", fields.size)
          properties.put("fields", fields)
          summaryData.put("properties", properties)

        case _ =>

      }
    }
    summaryResult = mapper.writeValueAsString(summaryData)
    summaryResult
  }

}