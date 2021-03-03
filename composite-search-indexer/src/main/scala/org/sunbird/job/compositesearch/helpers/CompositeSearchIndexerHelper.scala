package org.sunbird.job.compositesearch.helpers

import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.models.CompositeIndexer
import org.sunbird.job.util.{DefinitionUtil, ElasticSearchUtil, ScalaJsonUtil}

trait CompositeSearchIndexerHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[CompositeSearchIndexerHelper])
  lazy private val mapper = new ObjectMapper
  lazy private val definitionUtil = new DefinitionUtil

  def createCompositeSearchIndex()(esUtil: ElasticSearchUtil): Unit = {
    val settings = "{\"max_ngram_diff\":\"29\",\"mapping\":{\"total_fields\":{\"limit\":\"1500\"}},\"analysis\":{\"filter\":{\"mynGram\":{\"token_chars\":[\"letter\",\"digit\",\"whitespace\",\"punctuation\",\"symbol\"],\"min_gram\":\"1\",\"type\":\"nGram\",\"max_gram\":\"30\"}},\"analyzer\":{\"cs_index_analyzer\":{\"filter\":[\"lowercase\",\"mynGram\"],\"type\":\"custom\",\"tokenizer\":\"standard\"},\"keylower\":{\"filter\":\"lowercase\",\"tokenizer\":\"keyword\"},\"cs_search_analyzer\":{\"filter\":[\"standard\",\"lowercase\"],\"type\":\"custom\",\"tokenizer\":\"standard\"}}}}"
    val mappings = "{\"dynamic_templates\":[{\"nested\":{\"match_mapping_type\":\"object\",\"mapping\":{\"type\":\"nested\",\"fields\":{\"type\":\"nested\"}}}},{\"longs\":{\"match_mapping_type\":\"long\",\"mapping\":{\"type\":\"long\",\"fields\":{\"raw\":{\"type\":\"long\"}}}}},{\"booleans\":{\"match_mapping_type\":\"boolean\",\"mapping\":{\"type\":\"boolean\",\"fields\":{\"raw\":{\"type\":\"boolean\"}}}}},{\"doubles\":{\"match_mapping_type\":\"double\",\"mapping\":{\"type\":\"double\",\"fields\":{\"raw\":{\"type\":\"double\"}}}}},{\"dates\":{\"match_mapping_type\":\"date\",\"mapping\":{\"type\":\"date\",\"fields\":{\"raw\":{\"type\":\"date\"}}}}},{\"strings\":{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":\"text\",\"copy_to\":\"all_fields\",\"analyzer\":\"cs_index_analyzer\",\"search_analyzer\":\"cs_search_analyzer\",\"fields\":{\"raw\":{\"type\":\"text\",\"fielddata\":true,\"analyzer\":\"keylower\"}}}}}],\"properties\":{\"screenshots\":{\"type\":\"text\",\"index\":false},\"body\":{\"type\":\"text\",\"index\":false},\"appIcon\":{\"type\":\"text\",\"index\":false},\"all_fields\":{\"type\":\"text\",\"analyzer\":\"cs_index_analyzer\",\"search_analyzer\":\"cs_search_analyzer\",\"fields\":{\"raw\":{\"type\":\"text\",\"fielddata\":true,\"analyzer\":\"keylower\"}}}}}"
    esUtil.addIndex(settings, mappings)
  }

  private def getIndexDocument(id: String)(esUtil: ElasticSearchUtil): util.Map[String, AnyRef] = {
    val documentJson: String = esUtil.getDocumentAsStringById(id)
    val indexDocument = if (documentJson != null && !documentJson.isEmpty) ScalaJsonUtil.deserialize[util.Map[String, AnyRef]](documentJson) else new util.HashMap[String, AnyRef]()
    val indexDocument2 = if (documentJson != null && !documentJson.isEmpty) ScalaJsonUtil.deserialize[Map[String, AnyRef]](documentJson) else Map[String, AnyRef]()
    indexDocument
  }

  private def getIndexDocument(message: util.Map[String, Any], relationMap: Map[String, String], updateRequest: Boolean, externalProps: List[String], indexablePropslist: List[String], nestedFields: List[String])(esUtil: ElasticSearchUtil): util.Map[String, AnyRef] = {
    val uniqueId = message.get("nodeUniqueId").asInstanceOf[String]
    val indexDocument = if(updateRequest) getIndexDocument(uniqueId)(esUtil) else new util.HashMap[String, AnyRef]()
    val transactionData = message.getOrDefault("transactionData", new util.HashMap[String, Any]()).asInstanceOf[util.Map[String, Any]]
    if (!transactionData.isEmpty) {
      val addedProperties = transactionData.getOrDefault("properties", new util.HashMap[String, AnyRef]()).asInstanceOf[util.Map[String, AnyRef]]
      if (!indexablePropslist.isEmpty) {
        addedProperties.forEach((propKey, propValue) => {
          if (indexablePropslist.contains(propKey)) {
            addMetadataToDocument(propKey, propValue.asInstanceOf[util.Map[String, AnyRef]], indexDocument, nestedFields)
          }
        })
      } else {
        addedProperties.forEach((propKey, propValue) => {
          if (!externalProps.contains(propKey)) {
            addMetadataToDocument(propKey, propValue.asInstanceOf[util.Map[String, AnyRef]], indexDocument, nestedFields)
          }
        })
      }

      val addedRelations = transactionData.getOrDefault("addedRelations", new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[util.List[util.Map[String, AnyRef]]]
      if (!addedRelations.isEmpty) {
        addedRelations.forEach(rel => {
          val key = s"${rel.getOrDefault("dir", "").asInstanceOf[String]}_${rel.getOrDefault("type", "").asInstanceOf[String]}_${rel.getOrDefault("rel", "").asInstanceOf[String]}"
          val title = relationMap.getOrElse(key, "")
          if (!title.isEmpty) {
            val list = indexDocument.getOrDefault(title, new util.ArrayList[String]()).asInstanceOf[util.List[String]]
            val id = rel.getOrDefault("id", "").asInstanceOf[String]
            if (!list.contains(id)) {
              list.add(id)
              indexDocument.put(title, list)
            }
          }
        })
      }

      val removedRelations = transactionData.getOrDefault("removedRelations", new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[util.List[util.Map[String, AnyRef]]]
      if (!removedRelations.isEmpty) {
        removedRelations.forEach(rel => {
          val key = s"${rel.getOrDefault("dir", "").asInstanceOf[String]}_${rel.getOrDefault("type", "").asInstanceOf[String]}_${rel.getOrDefault("rel", "").asInstanceOf[String]}"
          val title = relationMap.getOrElse(key, "")
          if (!title.isEmpty) {
            val list = indexDocument.getOrDefault(title, new util.ArrayList[String]()).asInstanceOf[util.List[String]]
            if (!list.isEmpty) {
              val id = rel.getOrDefault("id", "").asInstanceOf[String]
              if (list.contains(id)) {
                list.remove(id)
                indexDocument.put(title, list)
              }
            }
          }
        })
      }
    }
    indexDocument.put("graph_id", message.get("graphId").asInstanceOf[String])
    indexDocument.put("node_id", message.get("nodeGraphId").asInstanceOf[Integer])
    indexDocument.put("identifier", message.get("nodeUniqueId").asInstanceOf[String])
    indexDocument.put("objectType", message.get("objectType").asInstanceOf[String])
    indexDocument.put("nodeType", message.get("nodeType").asInstanceOf[String])
    indexDocument
  }

  private def upsertDocument(uniqueId: String, jsonIndexDocument: String)(esUtil: ElasticSearchUtil): Unit = {
    esUtil.addDocumentWithId(uniqueId, jsonIndexDocument)
  }

  private def retrieveRelations(definitionNode: Map[String, AnyRef]): Map[String, String] = {
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
    val definition = definitionUtil.getDefinition(compositeObject.objectType, compositeObject.getVersionAsString(), compositeObject.getDefinitionBasePath())
    if (definition.isEmpty) {
      logger.info("Failed to fetch definition node from cache")
      throw new Exception(s"ERR_DEFINITION_NOT_FOUND: defnition node for graphId: ${compositeObject.graphId} and objectType:  ${compositeObject.objectType} is null due to some issue")
    }

    if (!compositeObject.getRestrictMetadataObjectTypes().contains(compositeObject.objectType)) {
      logger.info(s"Message Id: ${compositeObject.messageId}, Unique Id: ${compositeObject.uniqueId} is indexing into compositesearch.")
      val relationMap = retrieveRelations(definition)
      val externalProps = retrieveExternalProperties(definition)

      val indexablePropslist = if (compositeObject.getRestrictMetadataObjectTypes().contains(compositeObject.objectType)) getIndexableProperties(definition) else List[String]()
      upsertDocument(compositeObject.uniqueId, compositeObject.message, relationMap, externalProps, indexablePropslist, compositeObject.getNestedFields())(esUtil)
    } else {
      logger.info(s"Message Id: ${compositeObject.messageId}, Unique Id: ${compositeObject.uniqueId}, Object Type: ${compositeObject.objectType} is Not Indexable.")
    }
  }

  private def upsertDocument(uniqueId: String, message: util.Map[String, Any], relationMap: Map[String, String], externalProps: List[String], indexablePropslist: List[String], nestedFields: List[String])(esUtil: ElasticSearchUtil): Unit = {
    val operationType = message.getOrDefault("operationType", "").asInstanceOf[String]
    operationType match {
      case "CREATE" =>
        val indexDocument = getIndexDocument(message, relationMap, false, externalProps, indexablePropslist, nestedFields)(esUtil)
        val jsonIndexDocument = mapper.writeValueAsString(indexDocument)
        upsertDocument(uniqueId, jsonIndexDocument)(esUtil)
      case "UPDATE" =>
        val indexDocument = getIndexDocument(message, relationMap, true, externalProps, indexablePropslist, nestedFields)(esUtil)
        val jsonIndexDocument = mapper.writeValueAsString(indexDocument)
        upsertDocument(uniqueId, jsonIndexDocument)(esUtil)
      case "DELETE" =>
        val id = message.getOrDefault("nodeUniqueId", "").asInstanceOf[String]
        val indexDocument = getIndexDocument(id)(esUtil)
        val visibility = indexDocument.getOrDefault("visibility", "").asInstanceOf[String]
        if (StringUtils.equalsIgnoreCase("Parent", visibility)) logger.info(s"Not deleting the document (visibility: Parent) with ID: ${id}")
        else esUtil.deleteDocument(uniqueId)
      case _ =>
        logger.info(s"Unknown Operation Type : ${operationType} for the event.")
    }
  }

  private def getIndexableProperties(definition: Map[String, AnyRef]): List[String] = {
    val properties = definition.getOrElse("schema", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("properties", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    properties.filter(property => {
      property._2.asInstanceOf[Map[String, AnyRef]].getOrElse("indexed", false).asInstanceOf[Boolean]
    }).keys.toList
  }

  //
  //  private def getNonIndexableProperties(definition: Map[String, AnyRef]): List[String] = {
  //    val properties = definition.getOrElse("schema", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("properties", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
  //    properties.filter(property => {
  //      property._2.asInstanceOf[Map[String, AnyRef]].getOrElse("type", "").asInstanceOf[String].equalsIgnoreCase("object")
  //    }).keys.toList
  //  }

  private def retrieveExternalProperties(definition: Map[String, AnyRef]): List[String] = {
    val external = definition.getOrElse("config", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("external", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    val extProps = external.getOrElse("properties", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    extProps.keys.toList
  }

  private def addMetadataToDocument(propertyName: String, propertyMap: util.Map[String, AnyRef], indexDocument: util.Map[String, AnyRef], nestedFields: List[String]): Unit = {
    var propertyNewValue = propertyMap.getOrDefault("nv", null)
    if (propertyNewValue == null) {
      indexDocument.remove(propertyName)
    } else {
      if (nestedFields.contains(propertyName)) propertyNewValue = mapper.readValue(propertyNewValue.asInstanceOf[String], new TypeReference[AnyRef]() {})
      indexDocument.put(propertyName, propertyNewValue)
    }
  }

}
