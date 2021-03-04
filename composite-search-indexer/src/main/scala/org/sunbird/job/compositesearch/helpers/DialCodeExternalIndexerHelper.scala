package org.sunbird.job.compositesearch.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.util.{ElasticSearchUtil, ScalaJsonUtil}

import scala.collection.mutable

trait DialCodeExternalIndexerHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[DialCodeExternalIndexerHelper])

  def createDialCodeIndex()(esUitl: ElasticSearchUtil): Unit = {
    val settings: String = """{"max_ngram_diff":"29","mapping":{"total_fields":{"limit":"1050"}},"analysis":{"analyzer":{"dc_index_analyzer":{"type":"custom","tokenizer":"standard","filter":["lowercase","mynGram"]},"dc_search_analyzer":{"type":"custom","tokenizer":"standard","filter":["standard","lowercase"]},"keylower":{"tokenizer":"keyword","filter":"lowercase"}},"filter":{"mynGram":{"type":"nGram","min_gram":1,"max_gram":30,"token_chars":["letter","digit","whitespace","punctuation","symbol"]}}}}"""
    val mappings: String = """{"dynamic_templates":[{"longs":{"match_mapping_type":"long","mapping":{"type":"long","fields":{"raw":{"type":"long"}}}}},{"booleans":{"match_mapping_type":"boolean","mapping":{"type":"boolean","fields":{"raw":{"type":"boolean"}}}}},{"doubles":{"match_mapping_type":"double","mapping":{"type":"double","fields":{"raw":{"type":"double"}}}}},{"dates":{"match_mapping_type":"date","mapping":{"type":"date","fields":{"raw":{"type":"date"}}}}},{"strings":{"match_mapping_type":"string","mapping":{"type":"text","copy_to":"all_fields","analyzer":"dc_index_analyzer","search_analyzer":"dc_search_analyzer","fields":{"raw":{"type":"text","fielddata":true,"analyzer":"keylower"}}}}}],"properties":{"all_fields":{"type":"text","analyzer":"dc_index_analyzer","search_analyzer":"dc_search_analyzer","fields":{"raw":{"type":"text","fielddata":true,"analyzer":"keylower"}}}}}"""
    esUitl.addIndex(settings, mappings)
  }

  private def getIndexDocument(id: String)(esUtil: ElasticSearchUtil): mutable.Map[String, AnyRef] = {
    val documentJson: String = esUtil.getDocumentAsStringById(id)
    val indexDocument = if (documentJson != null && !documentJson.isEmpty) ScalaJsonUtil.deserialize[mutable.Map[String, AnyRef]](documentJson) else mutable.Map[String, AnyRef]()
    indexDocument
  }

  private def getIndexDocument(message: Map[String, Any], updateRequest: Boolean)(esUtil: ElasticSearchUtil): Map[String, AnyRef] = {
    val uniqueId: String = message.getOrElse("nodeUniqueId", "").asInstanceOf[String]
    val indexDocument = if (updateRequest) getIndexDocument(uniqueId)(esUtil) else mutable.Map[String, AnyRef]()
    val transactionData: Map[String, AnyRef] = message.getOrElse("transactionData", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    if (!transactionData.isEmpty) {
      val addedProperties: Map[String, AnyRef] = transactionData.getOrElse("properties", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      addedProperties.foreach(property => {
        val propertyNewValue: AnyRef = property._2.asInstanceOf[Map[String, AnyRef]].getOrElse("nv", null)
        if (propertyNewValue == null) indexDocument.remove(property._1) else indexDocument.put(property._1, propertyNewValue)
      })
    }
    indexDocument.put("identifier", message.getOrElse("nodeUniqueId", "").asInstanceOf[String])
    indexDocument.put("objectType", message.getOrElse("objectType", "").asInstanceOf[String])
    indexDocument.toMap
  }

  private def upsertDocument(uniqueId: String, jsonIndexDocument: String)(esUtil: ElasticSearchUtil): Unit = {
    esUtil.addDocumentWithId(uniqueId, jsonIndexDocument)
  }

  def upsertExternalDocument(uniqueId: String, message: Map[String, Any])(esUtil: ElasticSearchUtil): Unit = {
    logger.info(uniqueId + " is indexing into dialcode external.")
    val operationType: String = message.getOrElse("operationType", "").asInstanceOf[String]
    operationType match {
      case "CREATE" =>
        val indexDocument: Map[String, AnyRef] = getIndexDocument(message, false)(esUtil)
        val jsonIndexDocument: String = ScalaJsonUtil.serialize(indexDocument)
        upsertDocument(uniqueId, jsonIndexDocument)(esUtil)
      case "UPDATE" =>
        val indexDocument: Map[String, AnyRef] = getIndexDocument(message, true)(esUtil)
        val jsonIndexDocument: String = ScalaJsonUtil.serialize(indexDocument)
        upsertDocument(uniqueId, jsonIndexDocument)(esUtil)
      case "DELETE" =>
        esUtil.deleteDocument(uniqueId)
      case _ =>
        logger.info(s"Unknown Operation Type : ${operationType} for the event.")
    }
  }
}
