package org.sunbird.job.mvcindexer.util

import org.slf4j.LoggerFactory
import org.sunbird.job.exception.ElasticSearchException
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.util.{ElasticSearchUtil, JSONUtil}

import java.io.IOException


class MVCESIndexer(config: MVCIndexerConfig, esUtil: ElasticSearchUtil) {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[MVCESIndexer])
  private val NESTED_FIELDS = config.nestedFields

  /**
   * Create mvc index in Elasticsearch if not available
   */
  @throws[ElasticSearchException]
  def createMVCSearchIndex(): Unit = {
    try {
      esUtil.addIndex(config.indexSettings, config.indexMappings, config.indexAlias)
    } catch {
      case e: Exception =>
        throw new ElasticSearchException(s"Exception while creating index into in Elasticsearch - ${e.getMessage}", e)
    }

  }

  /**
   * Insert or update the document in ES based on the action. content id
   *
   * @param uniqueId Content ID
   * @param message  Event envelope
   */
  @throws[ElasticSearchException]
  def upsertDocument(uniqueId: String, message: Event): Unit = {
    try {
      var jsonIndexDocument: Map[String, AnyRef] = removeExtraParams(message.eventData)
      jsonIndexDocument ++= processNestedProps(jsonIndexDocument)
      val jsonAsString = JSONUtil.serialize(jsonIndexDocument)
      message.action match {
        case config.esIndexAction =>
          esUtil.addDocumentWithIndex(jsonAsString, config.mvcProcessorIndex, uniqueId)

        case config.contentRatingAction =>
          updateContentRating(uniqueId, message)

        case config.mlVectorAction =>
          updateContentTextVector(jsonIndexDocument, uniqueId, message)

        case config.mlKeywordAction =>
          esUtil.updateDocument(uniqueId, jsonAsString)
      }
    } catch {
      case e: Exception =>
        throw new ElasticSearchException(s"Exception while inserting data into ES for $uniqueId - ${e.getMessage}", e)
    }
  }

  def updateContentRating(uniqueId: String, message: Event): Unit = {
    val resp = esUtil.getDocumentAsString(uniqueId)
    if (null != resp && resp.contains(uniqueId)) {
      logger.info("Updating ES document with content rating for " + uniqueId)
      esUtil.updateDocument(uniqueId, JSONUtil.serialize(message.metadata))
    }
    else logger.info("Skipped updating content rating since ES document not found for " + uniqueId)
  }

  def updateContentTextVector(jsonIndexDocument: Map[String, AnyRef], uniqueId: String, message: Event): Unit = {
    val jsonAsString = JSONUtil.serialize(jsonIndexDocument + ("ml_contentTextVector" -> message.mlContentTextVector))
    esUtil.updateDocument(uniqueId, jsonAsString)
  }

  /**
   * Updating nested type props as null in ES
   *
   * @param jsonIndexDocument Content metadata
   * @return New map with available nested props as null
   */
  @throws[IOException]
  private def processNestedProps(jsonIndexDocument: Map[String, AnyRef]): Map[String, AnyRef] = {
    var nestedProps = Map[String, AnyRef]()
    for (propertyName <- jsonIndexDocument.keySet) {
      if (NESTED_FIELDS.contains(propertyName)) {
        val propertyNewValue = JSONUtil.deserialize[Map[String, AnyRef]](jsonIndexDocument(propertyName).asInstanceOf[String])
        nestedProps += (propertyName -> propertyNewValue)
      }
    }
    nestedProps
  }

  /**
   * Remove params which should not be inserted into ES from content metadata
   */
  def removeExtraParams(obj: Map[String, AnyRef]): Map[String, AnyRef] = {
    obj.-("action", "stage")
  }
}
