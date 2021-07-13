package org.sunbird.job.searchindexer.compositesearch.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.util.{ElasticSearchUtil, ScalaJsonUtil}

import scala.collection.mutable

trait DIALCodeMetricsIndexerHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[DIALCodeMetricsIndexerHelper])

  def createDialCodeIndex()(esUtil: ElasticSearchUtil): Boolean = {
    val settings: String = """{"number_of_shards":5}"""
    val mappings: String = """{"dcm":{"dynamic":false,"properties":{"dial_code":{"type":"keyword"},"total_dial_scans_local":{"type":"double"},"total_dial_scans_global":{"type":"double"},"average_scans_per_day":{"type":"double"},"last_scan":{"type":"date","format":"strict_date_optional_time||epoch_millis"},"first_scan":{"type":"date","format":"strict_date_optional_time||epoch_millis"}}}}"""
    esUtil.addIndex(settings, mappings)
  }

  private def getIndexDocument(id: String)(esUtil: ElasticSearchUtil): mutable.Map[String, AnyRef] = {
    val documentJson: String = esUtil.getDocumentAsString(id)
    val indexDocument = if (documentJson != null && !documentJson.isEmpty) ScalaJsonUtil.deserialize[mutable.Map[String, AnyRef]](documentJson) else mutable.Map[String, AnyRef]()
    indexDocument
  }

  def getIndexDocument(message: Map[String, Any], updateRequest: Boolean)(esUtil: ElasticSearchUtil): Map[String, AnyRef] = {
    val identifier: String = message.getOrElse("nodeUniqueId", "").asInstanceOf[String]
    val indexDocument = if (updateRequest) getIndexDocument(identifier)(esUtil) else mutable.Map[String, AnyRef]()
    val transactionData: Map[String, AnyRef] = message.getOrElse("transactionData", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    if (!transactionData.isEmpty) {
      val addedProperties: Map[String, AnyRef] = transactionData.getOrElse("properties", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      addedProperties.foreach(property => {
        val propertyNewValue: AnyRef = property._2.asInstanceOf[Map[String, AnyRef]].getOrElse("nv", null)
        if (propertyNewValue == null) indexDocument.remove(property._1) else indexDocument.put(property._1, propertyNewValue)
      })
    }
    indexDocument.put("dial_code", message.getOrElse("nodeUniqueId", "").asInstanceOf[String])
    indexDocument.put("objectType", message.getOrElse("objectType", "").asInstanceOf[String])
    indexDocument.toMap
  }

  private def upsertDocument(identifier: String, jsonIndexDocument: String)(esUtil: ElasticSearchUtil): Unit = {
    esUtil.addDocument(identifier, jsonIndexDocument)
  }

  def upsertDialcodeMetricDocument(identifier: String, message: Map[String, Any])(esUtil: ElasticSearchUtil): Unit = {
    val operationType: String = message.getOrElse("operationType", "").asInstanceOf[String]
    operationType match {
      case "CREATE" =>
        val indexDocument: Map[String, AnyRef] = getIndexDocument(message, false)(esUtil)
        val jsonIndexDocument: String = ScalaJsonUtil.serialize(indexDocument)
        upsertDocument(identifier, jsonIndexDocument)(esUtil)
      case "UPDATE" =>
        val indexDocument: Map[String, AnyRef] = getIndexDocument(message, true)(esUtil)
        val jsonIndexDocument: String = ScalaJsonUtil.serialize(indexDocument)
        upsertDocument(identifier, jsonIndexDocument)(esUtil)
      case "DELETE" =>
        esUtil.deleteDocument(identifier)
      case _ =>
        logger.info(s"Unknown Operation : ${operationType} for the identifier: ${identifier}.")
    }
  }
}
