package org.sunbird.job.compositesearch.helpers

import scala.collection.JavaConverters._
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.models.CompositeIndexer
import org.sunbird.job.util.{DefinitionCache, ElasticSearchUtil, ScalaJsonUtil}

import scala.collection.mutable

trait CompositeSearchIndexerHelper extends DefinitionCache {

  private[this] val logger = LoggerFactory.getLogger(classOf[CompositeSearchIndexerHelper])

  def createCompositeSearchIndex()(esUtil: ElasticSearchUtil): Boolean = {
    val settings = """{"max_ngram_diff":"29","mapping":{"total_fields":{"limit":"1500"}},"analysis":{"filter":{"mynGram":{"token_chars":["letter","digit","whitespace","punctuation","symbol"],"min_gram":"1","type":"nGram","max_gram":"30"}},"analyzer":{"cs_index_analyzer":{"filter":["lowercase","mynGram"],"type":"custom","tokenizer":"standard"},"keylower":{"filter":"lowercase","tokenizer":"keyword"},"cs_search_analyzer":{"filter":["standard","lowercase"],"type":"custom","tokenizer":"standard"}}}}"""
    val mappings = """{"dynamic_templates":[{"nested":{"match_mapping_type":"object","mapping":{"type":"nested","fields":{"type":"nested"}}}},{"longs":{"match_mapping_type":"long","mapping":{"type":"long","fields":{"raw":{"type":"long"}}}}},{"booleans":{"match_mapping_type":"boolean","mapping":{"type":"boolean","fields":{"raw":{"type":"boolean"}}}}},{"doubles":{"match_mapping_type":"double","mapping":{"type":"double","fields":{"raw":{"type":"double"}}}}},{"dates":{"match_mapping_type":"date","mapping":{"type":"date","fields":{"raw":{"type":"date"}}}}},{"strings":{"match_mapping_type":"string","mapping":{"type":"text","copy_to":"all_fields","analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer","fields":{"raw":{"type":"text","fielddata":true,"analyzer":"keylower"}}}}}],"properties":{"screenshots":{"type":"text","index":false},"body":{"type":"text","index":false},"appIcon":{"type":"text","index":false},"all_fields":{"type":"text","analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer","fields":{"raw":{"type":"text","fielddata":true,"analyzer":"keylower"}}}}}"""
    esUtil.addIndex(settings, mappings)
  }

  private def getIndexDocument(identifier: String)(esUtil: ElasticSearchUtil): mutable.Map[String, AnyRef] = {
    val documentJson: String = esUtil.getDocumentAsStringById(identifier)
    val indexDocument = if (documentJson != null && !documentJson.isEmpty) ScalaJsonUtil.deserialize[mutable.Map[String, AnyRef]](documentJson) else mutable.Map[String, AnyRef]()
    indexDocument
  }

  def getIndexDocument(message: Map[String, Any], relationMap: Map[String, String], updateRequest: Boolean, externalProps: List[String], indexablePropList: List[String], nestedFields: List[String])(esUtil: ElasticSearchUtil): Map[String, AnyRef] = {
    val identifier = message.getOrElse("nodeUniqueId", "").asInstanceOf[String]
    val indexDocument = if (updateRequest) getIndexDocument(identifier)(esUtil) else mutable.Map[String, AnyRef]()
    val transactionData = message.getOrElse("transactionData", Map[String, Any]()).asInstanceOf[Map[String, Any]]

    if (!transactionData.isEmpty) {
      val addedProperties = transactionData.getOrElse("properties", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      addedProperties.foreach(property => {
        if (!indexablePropList.isEmpty) {
          if (indexablePropList.contains(property._1)) {
            val propertyNewValue: AnyRef = property._2.asInstanceOf[Map[String, AnyRef]].getOrElse("nv", null)
            if (propertyNewValue == null) indexDocument.remove(property._1) else indexDocument.put(property._1, addMetadataToDocument(property._1, propertyNewValue, nestedFields))
          }
        } else {
          if (!externalProps.contains(property._1)) {
            val propertyNewValue: AnyRef = property._2.asInstanceOf[Map[String, AnyRef]].getOrElse("nv", null)
            if (propertyNewValue == null) indexDocument.remove(property._1) else indexDocument.put(property._1, addMetadataToDocument(property._1, propertyNewValue, nestedFields))
          }
        }
      })

      val addedRelations = transactionData.getOrElse("addedRelations", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
      if (!addedRelations.isEmpty) {

        addedRelations.foreach(rel => {
          val key = s"${rel.getOrElse("dir", "").asInstanceOf[String]}_${rel.getOrElse("type", "").asInstanceOf[String]}_${rel.getOrElse("rel", "").asInstanceOf[String]}"
          val title = relationMap.getOrElse(key, "")
          if (!title.isEmpty) {
            val list = indexDocument.getOrElse(title, List[String]()).asInstanceOf[List[String]]
            val id = rel.getOrElse("id", "").asInstanceOf[String]
            if (!list.contains(id)) indexDocument.put(title, (id :: list).asInstanceOf[AnyRef])
          }
        })
      }

      val removedRelations = transactionData.getOrElse("removedRelations", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
      removedRelations.foreach(rel => {
        val key = s"${rel.getOrElse("dir", "").asInstanceOf[String]}_${rel.getOrElse("type", "").asInstanceOf[String]}_${rel.getOrElse("rel", "").asInstanceOf[String]}"
        val title = relationMap.getOrElse(key, "")
        if (!title.isEmpty) {
          val list = indexDocument.getOrElse(title, List[String]()).asInstanceOf[List[String]].to[mutable.ListBuffer]
          val id = rel.getOrElse("id", "").asInstanceOf[String]
          if (list.contains(id)) {
            list -= id
            indexDocument.put(title, list.toList.asInstanceOf[AnyRef])
          }
        }
      })
    }

    indexDocument.put("graph_id", message.getOrElse("graphId", "").asInstanceOf[String])
    indexDocument.put("node_id", message.getOrElse("nodeGraphId", 0).asInstanceOf[Integer])
    indexDocument.put("identifier", message.getOrElse("nodeUniqueId", "").asInstanceOf[String])
    indexDocument.put("objectType", message.getOrElse("objectType", "").asInstanceOf[String])
    indexDocument.put("nodeType", message.getOrElse("nodeType", "").asInstanceOf[String])
    indexDocument.toMap
  }

  def upsertDocument(identifier: String, jsonIndexDocument: String)(esUtil: ElasticSearchUtil): Unit = {
    esUtil.addDocumentWithId(identifier, jsonIndexDocument)
  }

  def retrieveRelations(definitionNode: Map[String, AnyRef]): Map[String, String] = {
    val relations = definitionNode.getOrElse("config", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("relations", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    relations.flatMap(r => {
      val relation = r._2.asInstanceOf[Map[String, AnyRef]]
      val direction = relation.getOrElse("direction", "").asInstanceOf[String].toUpperCase
      val relType = relation.getOrElse("type", "").asInstanceOf[String]
      val objectTypes = relation.getOrElse("objects", List[String]()).asInstanceOf[List[String]]
      objectTypes.flatMap(objType => {
        Map(s"${direction}_${objType}_${relType}" -> r._1)
      }).toMap
    })
  }

  def processESMessage(compositeObject: CompositeIndexer)(esUtil: ElasticSearchUtil): Unit = {
    val definition = getDefinition(compositeObject.objectType, compositeObject.getVersionAsString(), compositeObject.getDefinitionBasePath())
    val relationMap = retrieveRelations(definition)
    val externalProps = retrieveExternalProperties(definition)
    val indexablePropertiesList = if (compositeObject.getRestrictMetadataObjectTypes().contains(compositeObject.objectType)) getIndexableProperties(definition) else List[String]()
    val compositeMap = compositeObject.message.asScala.toMap
    upsertDocument(compositeObject.identifier, compositeMap, relationMap, externalProps, indexablePropertiesList, compositeObject.getNestedFields())(esUtil)
  }


  private def upsertDocument(identifier: String, message: Map[String, Any], relationMap: Map[String, String], externalProps: List[String], indexablePropList: List[String], nestedFields: List[String])(esUtil: ElasticSearchUtil): Unit = {
    val operationType = message.getOrElse("operationType", "").asInstanceOf[String]
    operationType match {
      case "CREATE" =>
        val indexDocument = getIndexDocument(message, relationMap, false, externalProps, indexablePropList, nestedFields)(esUtil)
        val jsonIndexDocument = ScalaJsonUtil.serialize(indexDocument)
        upsertDocument(identifier, jsonIndexDocument)(esUtil)
      case "UPDATE" =>
        val indexDocument = getIndexDocument(message, relationMap, true, externalProps, indexablePropList, nestedFields)(esUtil)
        val jsonIndexDocument = ScalaJsonUtil.serialize(indexDocument)
        upsertDocument(identifier, jsonIndexDocument)(esUtil)
      case "DELETE" =>
        val id = message.getOrElse("nodeUniqueId", "").asInstanceOf[String]
        val indexDocument = getIndexDocument(id)(esUtil)
        val visibility = indexDocument.getOrElse("visibility", "").asInstanceOf[String]
        if (StringUtils.equalsIgnoreCase("Parent", visibility)) logger.info(s"Not deleting the document (visibility: Parent) with ID: ${id}")
        else esUtil.deleteDocument(identifier)
      case _ =>
        logger.info(s"Unknown Operation Type : ${operationType} for the identifier: ${identifier}.")
    }
  }

  def getIndexableProperties(definition: Map[String, AnyRef]): List[String] = {
    val properties = definition.getOrElse("schema", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("properties", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    properties.filter(property => {
      property._2.asInstanceOf[Map[String, AnyRef]].getOrElse("indexed", false).asInstanceOf[Boolean]
    }).keys.toList
  }

  def retrieveExternalProperties(definition: Map[String, AnyRef]): List[String] = {
    val external = definition.getOrElse("config", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("external", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    val extProps = external.getOrElse("properties", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    extProps.keys.toList
  }

  private def addMetadataToDocument(propertyName: String, propertyValue: AnyRef, nestedFields: List[String]): AnyRef = {
    val propertyNewValue = if (nestedFields.contains(propertyName)) ScalaJsonUtil.deserialize[AnyRef](propertyValue.asInstanceOf[String]) else propertyValue
    propertyNewValue
  }

}
