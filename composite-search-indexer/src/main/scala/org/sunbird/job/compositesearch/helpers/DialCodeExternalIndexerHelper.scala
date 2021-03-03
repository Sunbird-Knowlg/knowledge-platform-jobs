package org.sunbird.job.compositesearch.helpers

import java.util
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.task.CompositeSearchIndexerConfig
import org.sunbird.job.util.ElasticSearchUtil

trait DialCodeExternalIndexerHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[DialCodeExternalIndexerHelper])
  lazy private val mapper = new ObjectMapper

  def createDialCodeIndex()(esUitl: ElasticSearchUtil): Unit = {
    val settings: String = "{\"max_ngram_diff\":\"29\",\"mapping\":{\"total_fields\":{\"limit\":\"1050\"}},\"analysis\":{\"analyzer\":{\"dc_index_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"lowercase\",\"mynGram\"]},\"dc_search_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"standard\",\"lowercase\"]},\"keylower\":{\"tokenizer\":\"keyword\",\"filter\":\"lowercase\"}},\"filter\":{\"mynGram\":{\"type\":\"nGram\",\"min_gram\":1,\"max_gram\":30,\"token_chars\":[\"letter\",\"digit\",\"whitespace\",\"punctuation\",\"symbol\"]}}}}"
    val mappings: String = "{\"dynamic_templates\":[{\"longs\":{\"match_mapping_type\":\"long\",\"mapping\":{\"type\":\"long\",\"fields\":{\"raw\":{\"type\":\"long\"}}}}},{\"booleans\":{\"match_mapping_type\":\"boolean\",\"mapping\":{\"type\":\"boolean\",\"fields\":{\"raw\":{\"type\":\"boolean\"}}}}},{\"doubles\":{\"match_mapping_type\":\"double\",\"mapping\":{\"type\":\"double\",\"fields\":{\"raw\":{\"type\":\"double\"}}}}},{\"dates\":{\"match_mapping_type\":\"date\",\"mapping\":{\"type\":\"date\",\"fields\":{\"raw\":{\"type\":\"date\"}}}}},{\"strings\":{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":\"text\",\"copy_to\":\"all_fields\",\"analyzer\":\"dc_index_analyzer\",\"search_analyzer\":\"dc_search_analyzer\",\"fields\":{\"raw\":{\"type\":\"text\",\"fielddata\":true,\"analyzer\":\"keylower\"}}}}}],\"properties\":{\"all_fields\":{\"type\":\"text\",\"analyzer\":\"dc_index_analyzer\",\"search_analyzer\":\"dc_search_analyzer\",\"fields\":{\"raw\":{\"type\":\"text\",\"fielddata\":true,\"analyzer\":\"keylower\"}}}}}"
    esUitl.addIndex(settings, mappings)
  }

  private def getIndexDocument(message: util.Map[String, Any], updateRequest: Boolean)(esUtil: ElasticSearchUtil): util.Map[String, AnyRef] = {
    var indexDocument: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    if (updateRequest) {
      val uniqueId: String = message.get("nodeUniqueId").asInstanceOf[String]
      val documentJson: String = esUtil.getDocumentAsStringById(uniqueId)
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
    indexDocument.put("identifier", message.get("nodeUniqueId").asInstanceOf[String])
    indexDocument.put("objectType", message.get("objectType").asInstanceOf[String])
    indexDocument
  }

  private def upsertDocument(uniqueId: String, jsonIndexDocument: String)(esUtil: ElasticSearchUtil): Unit = {
    esUtil.addDocumentWithId(uniqueId, jsonIndexDocument)
  }

  def upsertExternalDocument(uniqueId: String, message: util.Map[String, Any])(esUtil: ElasticSearchUtil): Unit = {
    logger.info(uniqueId + " is indexing into dialcode external.")
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
