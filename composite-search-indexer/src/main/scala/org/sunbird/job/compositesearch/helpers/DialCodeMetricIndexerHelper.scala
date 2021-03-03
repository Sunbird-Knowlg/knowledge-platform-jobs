package org.sunbird.job.compositesearch.helpers

import java.util
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.task.CompositeSearchIndexerConfig
import org.sunbird.job.util.ElasticSearchUtil

trait DialCodeMetricIndexerHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[DialCodeMetricIndexerHelper])
  lazy private val mapper = new ObjectMapper

  def createDialCodeIndex()(esUitl: ElasticSearchUtil): Unit = {
    val settings: String = "{\"number_of_shards\":5}"
    val mappings: String = "{\"dcm\":{\"dynamic\":false,\"properties\":{\"dial_code\":{\"type\":\"keyword\"},\"total_dial_scans_local\":{\"type\":\"double\"},\"total_dial_scans_global\":{\"type\":\"double\"},\"average_scans_per_day\":{\"type\":\"double\"},\"last_scan\":{\"type\":\"date\",\"format\":\"strict_date_optional_time||epoch_millis\"},\"first_scan\":{\"type\":\"date\",\"format\":\"strict_date_optional_time||epoch_millis\"}}}}"
    esUitl.addIndex(settings, mappings)
  }

  private def getIndexDocument(message: util.Map[String, Any], updateRequest: Boolean)(esUitl: ElasticSearchUtil): util.Map[String, AnyRef] = {
    var indexDocument: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    if (updateRequest) {
      val uniqueId: String = message.get("nodeUniqueId").asInstanceOf[String]
      val documentJson: String = esUitl.getDocumentAsStringById(uniqueId)
      if (documentJson != null && !documentJson.isEmpty) indexDocument = mapper.readValue(documentJson, new TypeReference[util.Map[String, AnyRef]]() {})
    }
    val transactionData: util.Map[String, AnyRef] = message.get("transactionData").asInstanceOf[util.Map[String, AnyRef]]
    if (transactionData != null) {
      val addedProperties: util.Map[String, AnyRef] = transactionData.get("properties").asInstanceOf[util.Map[String, AnyRef]]
      if (addedProperties != null && !addedProperties.isEmpty) {
        addedProperties.entrySet.forEach(propertyMap => {
          if (propertyMap != null && propertyMap.getKey != null) {
            val propertyName: String = propertyMap.getKey
            val propertyNewValue: AnyRef = propertyMap.getValue.asInstanceOf[util.Map[String, AnyRef]].get("nv")
            if (propertyNewValue == null) indexDocument.remove(propertyName)
            else indexDocument.put(propertyName, propertyNewValue)
          }
        })
      }
    }
    indexDocument.put("dial_code", message.get("nodeUniqueId").asInstanceOf[AnyRef])
    indexDocument.put("objectType", message.get("objectType").asInstanceOf[AnyRef])
    indexDocument
  }

  private def upsertDocument(uniqueId: String, jsonIndexDocument: String)(esUitl: ElasticSearchUtil): Unit = {
    esUitl.addDocumentWithId(uniqueId, jsonIndexDocument)
    logger.info(s"Indexed dialcode metrics successfully for dialcode : ${uniqueId}")
  }

  def upsertDialcodeMetricDocument(uniqueId: String, message: util.Map[String, Any])(esUtil: ElasticSearchUtil): Unit = {
    logger.info(s"${uniqueId} is indexing into dialcodemetrics.")
    val operationType: String = message.getOrDefault("operationType", "").asInstanceOf[String]
    operationType match {
      case "CREATE" =>
        val indexDocument: util.Map[String, AnyRef] = getIndexDocument(message, false)(esUtil)
        val jsonIndexDocument: String = mapper.writeValueAsString(indexDocument)
        upsertDocument(uniqueId, jsonIndexDocument)(esUtil)
      case "UPDATE" =>
        val indexDocument: util.Map[String, AnyRef] = getIndexDocument(message, true)(esUtil)
        val jsonIndexDocument: String = mapper.writeValueAsString(indexDocument)
        upsertDocument(uniqueId, jsonIndexDocument)(esUtil)
      case "DELETE" =>
        esUtil.deleteDocument(uniqueId)
      case _ =>
        logger.info(s"Unknown Operation Type : ${operationType} for the event.")
    }
  }
}
