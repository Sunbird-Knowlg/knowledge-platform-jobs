package org.sunbird.job.content.publish.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.ObjectDefinition
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.util.{ElasticSearchUtil, ScalaJsonUtil}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait SyncMessagesGenerator {

  private[this] val logger = LoggerFactory.getLogger(classOf[SyncMessagesGenerator])

  private def getIndexDocument(identifier: String)(esUtil: ElasticSearchUtil): scala.collection.mutable.Map[String, AnyRef] = {
    val documentJson: String = esUtil.getDocumentAsString(identifier)
    if (documentJson != null && documentJson.nonEmpty) ScalaJsonUtil.deserialize[scala.collection.mutable.Map[String, AnyRef]](documentJson) else scala.collection.mutable.Map[String, AnyRef]()
  }

  private def getJsonMessage(message: Map[String, Any], definition: ObjectDefinition, nestedFields: List[String], ignoredFields: List[String]): Map[String, AnyRef] = {
    val indexDocument = scala.collection.mutable.Map[String, AnyRef]()
    val transactionData = message.getOrElse("transactionData", Map[String, Any]()).asInstanceOf[Map[String, Any]]
    logger.debug("SyncMessagesGenerator:: getJsonMessage:: transactionData:: " + transactionData)
    if (transactionData.nonEmpty) {
      val addedProperties = transactionData.getOrElse("properties", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      logger.debug("SyncMessagesGenerator:: getJsonMessage:: definition.externalProperties:: " + definition.externalProperties)
      addedProperties.foreach(property => {
        if (!definition.externalProperties.contains(property._1)) {
          val propertyNewValue: AnyRef = property._2.asInstanceOf[Map[String, AnyRef]].getOrElse("nv", null)
          if (propertyNewValue == null) indexDocument.remove(property._1) else indexDocument.put(property._1, addMetadataToDocument(property._1, propertyNewValue, nestedFields))
        }
      })

//      val addedRelations = transactionData.getOrElse("addedRelations", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
//      if (addedRelations.nonEmpty) {
//        addedRelations.foreach(rel => {
//          val direction = rel.getOrElse("dir", "").asInstanceOf[String]
//          val relationType = rel.getOrElse("rel", "").asInstanceOf[String]
//          val targetObjType = rel.getOrElse("type", "").asInstanceOf[String]
//          val title = definition.relationLabel(targetObjType, direction, relationType)
//          if (title.nonEmpty) {
//            val list = indexDocument.getOrElse(title.get, List[String]()).asInstanceOf[List[String]]
//            val id = rel.getOrElse("id", "").asInstanceOf[String]
//            if (!list.contains(id)) indexDocument.put(title.get, (id :: list).asInstanceOf[AnyRef])
//          }
//        })
//      }
//
//      val removedRelations = transactionData.getOrElse("removedRelations", List[Map[String, AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
//      removedRelations.foreach(rel => {
//        val direction = rel.getOrElse("dir", "").asInstanceOf[String]
//        val relationType = rel.getOrElse("rel", "").asInstanceOf[String]
//        val targetObjType = rel.getOrElse("type", "").asInstanceOf[String]
//        val title = definition.relationLabel(targetObjType, direction, relationType)
//        if (title.nonEmpty) {
//          val list = indexDocument.getOrElse(title.get, List[String]()).asInstanceOf[List[String]]
//          val id = rel.getOrElse("id", "").asInstanceOf[String]
//          if (list.contains(id)) {
//            val updatedList =  list diff List(id)
//            indexDocument.put(title.get, updatedList.asInstanceOf[AnyRef])
//          }
//        }
//      })
    }

    //Ignored fields are removed-> it can be a propertyName or relation Name
    indexDocument --= ignoredFields

    indexDocument.put("graph_id", message.getOrElse("graphId", "domain").asInstanceOf[String])
    indexDocument.put("node_id", message.getOrElse("nodeGraphId",0).asInstanceOf[AnyRef])
    indexDocument.put("identifier", message.getOrElse("nodeUniqueId", "").asInstanceOf[String])
    indexDocument.put("objectType", message.getOrElse("objectType", "").asInstanceOf[String])
    indexDocument.put("nodeType", message.getOrElse("nodeType", "").asInstanceOf[String])

    logger.info("SyncMessagesGenerator:: getJsonMessage:: final indexDocument:: " + indexDocument)

    indexDocument.toMap
  }

  private def addMetadataToDocument(propertyName: String, propertyValue: AnyRef, nestedFields: List[String]): AnyRef = {
    if (nestedFields.contains(propertyName)) {
      propertyValue match {
        case propVal: String => ScalaJsonUtil.deserialize[AnyRef](propVal)
        case _ => propertyValue
      }
    } else propertyValue
  }

  def getMessages(nodes: List[ObjectData], definition: ObjectDefinition, nestedFields: List[String], errors: mutable.Map[String, String])(esUtil: ElasticSearchUtil): Map[String, Map[String, AnyRef]] = {
    val messages = collection.mutable.Map.empty[String, Map[String, AnyRef]]
    for (node <- nodes) {
      try {
        if (definition.getRelationLabels() != null) {
          val nodeMap = getNodeMap(node)
          logger.debug("SyncMessagesGenerator:: getMessages:: nodeMap:: " + nodeMap)
          val message = getJsonMessage(nodeMap, definition, nestedFields, List.empty)
          logger.debug("SyncMessagesGenerator:: getMessages:: message:: " + message)
          messages.put(node.identifier, message)
        }
      } catch {
        case e: Exception => e.printStackTrace()
          errors.put(node.identifier, e.getMessage)
      }
    }
    messages.toMap
  }


  private def getNodeMap(node: ObjectData): Map[String, AnyRef] = {
    val transactionData = collection.mutable.Map.empty[String, AnyRef]
    if (null != node.metadata && node.metadata.nonEmpty) {
      val propertyMap = collection.mutable.Map.empty[String, AnyRef]
      for (key <- node.metadata.keySet) {
        if (StringUtils.isNotBlank(key)) {
          val valueMap = collection.mutable.Map.empty[String, AnyRef]
          valueMap.put("ov", null) // old value
          valueMap.put("nv", node.metadata(key)) // new value

          // temporary check to not sync body and editorState
          if (!StringUtils.equalsIgnoreCase("body", key) && !StringUtils.equalsIgnoreCase("editorState", key)) propertyMap.put(key, valueMap.toMap)
        }
      }
      transactionData.put("properties", propertyMap.toMap)
    }
    else transactionData.put("properties", Map.empty[String,AnyRef])

    val relations = ListBuffer.empty[Map[String, AnyRef]]
    // add IN relations
//    if (null != node.metadata.inRelations && node.metadata.inRelations.nonEmpty) {
//      for (rel <- node.metadata.inRelations) {
//        val relMap = Map("rel" -> rel.relationType, "id" -> rel.startNodeId, "dir" -> "IN", "type" -> rel.startNodeObjectType, "label" -> getLabel(rel.getStartNodeMetadata))
//        relations += relMap
//      }
//    }
//    // add OUT relations
//    if (null != node.getOutRelations && !node.getOutRelations.isEmpty) {
//      for (rel <- node.getOutRelations) {
//        val relMap = Map("rel" -> rel.getRelationType, "id" -> rel.getEndNodeId, "dir" -> "OUT", "type" -> rel.getEndNodeObjectType, "label" -> getLabel(rel.getEndNodeMetadata))
//        relations += relMap
//      }
//    }
    transactionData.put("addedRelations", relations.toList)

     Map("operationType"-> "UPDATE", "graphId" -> node.metadata.getOrElse("graphId","domain").asInstanceOf[String], "nodeGraphId"-> 0.asInstanceOf[AnyRef], "nodeUniqueId"-> node.identifier, "objectType"-> node.dbObjType,
      "nodeType"-> "DATA_NODE", "transactionData" -> transactionData.toMap, "syncMessage" -> true.asInstanceOf[AnyRef])
  }


  def getDocument(node: ObjectData, updateRequest: Boolean, nestedFields: List[String])(esUtil: ElasticSearchUtil): Map[String, AnyRef] = {
    val message = getNodeMap(node)
    val identifier: String = message.getOrElse("nodeUniqueId", "").asInstanceOf[String]
    val indexDocument = mutable.Map[String, AnyRef]()
    val transactionData: Map[String, AnyRef] = message.getOrElse("transactionData", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    if (transactionData.nonEmpty) {
      val addedProperties: Map[String, AnyRef] = transactionData.getOrElse("properties", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
      addedProperties.foreach(property => {
        val propertyNewValue: AnyRef = property._2.asInstanceOf[Map[String, AnyRef]].getOrElse("nv", null)
        if (propertyNewValue == null) indexDocument.remove(property._1) else indexDocument.put(property._1, addMetadataToDocument(property._1, propertyNewValue, nestedFields))
      })
    }
    indexDocument.put("identifier", message.getOrElse("nodeUniqueId", "").asInstanceOf[String])
    indexDocument.put("objectType", message.getOrElse("objectType", "").asInstanceOf[String])
    indexDocument.toMap
  }

}
