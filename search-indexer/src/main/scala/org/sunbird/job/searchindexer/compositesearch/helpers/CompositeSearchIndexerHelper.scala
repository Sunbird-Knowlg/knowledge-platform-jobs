package org.sunbird.job.searchindexer.compositesearch.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.searchindexer.models.CompositeIndexer
import org.sunbird.job.util.{ElasticSearchUtil, ScalaJsonUtil}

import scala.collection.JavaConverters._

trait CompositeSearchIndexerHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[CompositeSearchIndexerHelper])

  def createCompositeSearchIndex()(esUtil: ElasticSearchUtil): Boolean = {
    val settings = """{"max_ngram_diff":"29","mapping":{"total_fields":{"limit":"1500"}},"analysis":{"filter":{"mynGram":{"token_chars":["letter","digit","whitespace","punctuation","symbol"],"min_gram":"1","type":"nGram","max_gram":"30"}},"analyzer":{"cs_index_analyzer":{"filter":["lowercase","mynGram"],"type":"custom","tokenizer":"standard"},"keylower":{"filter":"lowercase","tokenizer":"keyword"},"cs_search_analyzer":{"filter":["standard","lowercase"],"type":"custom","tokenizer":"standard"}}}}"""
    val mappings = """{"dynamic_templates":[{"nested":{"match_mapping_type":"object","mapping":{"type":"nested","fields":{"type":"nested"}}}},{"longs":{"match_mapping_type":"long","mapping":{"type":"long","fields":{"raw":{"type":"long"}}}}},{"booleans":{"match_mapping_type":"boolean","mapping":{"type":"boolean","fields":{"raw":{"type":"boolean"}}}}},{"doubles":{"match_mapping_type":"double","mapping":{"type":"double","fields":{"raw":{"type":"double"}}}}},{"dates":{"match_mapping_type":"date","mapping":{"type":"date","fields":{"raw":{"type":"date"}}}}},{"strings":{"match_mapping_type":"string","mapping":{"type":"text","copy_to":"all_fields","analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer","fields":{"raw":{"type":"text","fielddata":true,"analyzer":"keylower"}}}}}],"properties":{"screenshots":{"type":"text","index":false},"body":{"type":"text","index":false},"appIcon":{"type":"text","index":false},"all_fields":{"type":"text","analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer","fields":{"raw":{"type":"text","fielddata":true,"analyzer":"keylower"}}}}}"""
    esUtil.addIndex(settings, mappings)
  }

  private def getIndexDocument(identifier: String)(esUtil: ElasticSearchUtil): scala.collection.mutable.Map[String, AnyRef] = {
    val documentJson: String = esUtil.getDocumentAsString(identifier)
    val indexDocument = if (documentJson != null && documentJson.nonEmpty) ScalaJsonUtil.deserialize[scala.collection.mutable.Map[String, AnyRef]](documentJson) else scala.collection.mutable.Map[String, AnyRef]()
    indexDocument
  }

  def getIndexDocument(message: Map[String, Any], isUpdate: Boolean, definition: ObjectDefinition, nestedFields: List[String], ignoredFields: List[String])(esUtil: ElasticSearchUtil): Map[String, AnyRef] = {
    val identifier = message.getOrElse("nodeUniqueId", "").asInstanceOf[String]
    val indexDocument = if (isUpdate) getIndexDocument(identifier)(esUtil) else scala.collection.mutable.Map[String, AnyRef]()
    val transactionData = message.getOrElse("transactionData", Map[String, Any]()).asInstanceOf[Map[String, Any]]

    if (transactionData.nonEmpty) {
      val addedProperties = transactionData.getOrElse("properties", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      addedProperties.foreach(property => {
        if (!definition.externalProperties.contains(property._1)) {
          val propertyNewValue: AnyRef = property._2.asInstanceOf[Map[String, AnyRef]].getOrElse("nv", null) match {
            case propVal: List[AnyRef] => if(propVal.isEmpty) null else propVal
            case _ => property._2.asInstanceOf[Map[String, AnyRef]].getOrElse("nv", null)
          }
          if (propertyNewValue == null) indexDocument.remove(property._1) else indexDocument.put(property._1, addMetadataToDocument(property._1, propertyNewValue, nestedFields))
        }
      })

      val addedRelations = transactionData.getOrElse("addedRelations", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
      if (addedRelations.nonEmpty) {
        addedRelations.foreach(rel => {
          val direction = rel.getOrElse("dir", "").asInstanceOf[String]
          val relationType = rel.getOrElse("rel", "").asInstanceOf[String]
          val targetObjType = rel.getOrElse("type", "").asInstanceOf[String]
          val title = definition.relationLabel(targetObjType, direction, relationType)
          if (title.nonEmpty) {
            val list = indexDocument.getOrElse(title.get, List[String]()).asInstanceOf[List[String]]
            val id = rel.getOrElse("id", "").asInstanceOf[String]
            if (!list.contains(id)) indexDocument.put(title.get, (id :: list).asInstanceOf[AnyRef])
          }
        })
      }

      val removedRelations = transactionData.getOrElse("removedRelations", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
      removedRelations.foreach(rel => {
        val direction = rel.getOrElse("dir", "").asInstanceOf[String]
        val relationType = rel.getOrElse("rel", "").asInstanceOf[String]
        val targetObjType = rel.getOrElse("type", "").asInstanceOf[String]
        val title = definition.relationLabel(targetObjType, direction, relationType)
        if (title.nonEmpty) {
          val list = indexDocument.getOrElse(title.get, List[String]()).asInstanceOf[List[String]]
          val id = rel.getOrElse("id", "").asInstanceOf[String]
          if (list.contains(id)) {
            val updatedList =  list diff List(id)
            indexDocument.put(title.get, updatedList.asInstanceOf[AnyRef])
          }
        }
      })
    }
    
    //Ignored fields are removed-> it can be a propertyName or relation Name
    indexDocument --= ignoredFields

    indexDocument.put("graph_id", message.getOrElse("graphId", "").asInstanceOf[String])
    indexDocument.put("node_id", message.getOrElse("nodeGraphId", 0).asInstanceOf[Integer])
    indexDocument.put("identifier", message.getOrElse("nodeUniqueId", "").asInstanceOf[String])
    indexDocument.put("objectType", message.getOrElse("objectType", "").asInstanceOf[String])
    indexDocument.put("nodeType", message.getOrElse("nodeType", "").asInstanceOf[String])
    indexDocument.toMap
  }

  def upsertDocument(identifier: String, jsonIndexDocument: String)(esUtil: ElasticSearchUtil): Unit = {
    esUtil.addDocument(identifier, jsonIndexDocument)
  }

  def processESMessage(compositeObject: CompositeIndexer)(esUtil: ElasticSearchUtil, defCache: DefinitionCache): Unit = {
    val definition = defCache.getDefinition(compositeObject.objectType, compositeObject.getVersionAsString(), compositeObject.getDefinitionBasePath())

    val compositeMap = compositeObject.message.asScala.toMap
    upsertDocument(compositeObject.identifier, compositeMap, definition, compositeObject.getNestedFields(), compositeObject.getIgnoredFields())(esUtil)
  }


  private def upsertDocument(identifier: String, message: Map[String, Any], definition: ObjectDefinition, nestedFields: List[String], ignoredFields: List[String])(esUtil: ElasticSearchUtil): Unit = {
    val operationType = message.getOrElse("operationType", "").asInstanceOf[String]
    operationType match {
      case "CREATE" =>
        val indexDocument = getIndexDocument(message, false, definition, nestedFields, ignoredFields)(esUtil)
        val jsonIndexDocument = ScalaJsonUtil.serialize(indexDocument)
        upsertDocument(identifier, jsonIndexDocument)(esUtil)
      case "UPDATE" =>
        val indexDocument = getIndexDocument(message, true, definition, nestedFields, ignoredFields)(esUtil)
        val jsonIndexDocument = ScalaJsonUtil.serialize(indexDocument)
        upsertDocument(identifier, jsonIndexDocument)(esUtil)
      case "DELETE" =>
        val id = message.getOrElse("nodeUniqueId", "").asInstanceOf[String]
        val indexDocument = getIndexDocument(id)(esUtil)
        val visibility = indexDocument.getOrElse("visibility", "").asInstanceOf[String]
        if (StringUtils.equalsIgnoreCase("Parent", visibility)) logger.info(s"Not deleting the document (visibility: Parent) with ID: $id")
        else esUtil.deleteDocument(identifier)
      case _ =>
        logger.info(s"Unknown Operation Type : $operationType for the identifier: $identifier.")
    }
  }

  private def addMetadataToDocument(propertyName: String, propertyValue: AnyRef, nestedFields: List[String]): AnyRef = {
    val propertyNewValue = if (nestedFields.contains(propertyName)) ScalaJsonUtil.deserialize[AnyRef](propertyValue.asInstanceOf[String]) else propertyValue
    propertyNewValue
  }

}
