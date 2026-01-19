package org.sunbird.job.knowlg.publish.helpers

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.util.{CassandraUtil, ScalaJsonUtil}

import scala.collection.JavaConverters._

/**
 * Helper class for managing content hierarchy relationships in Cassandra
 * Handles leafnodes, ancestors, and optionalnodes relationships for collections
 */
class HierarchyRelationshipHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[HierarchyRelationshipHelper])

  /**
   * Updates hierarchy relationships for a published collection
   * @param obj Published collection object data
   * @param cassandraUtil Cassandra utility instance
   * @param config Configuration object
   */
  def updateHierarchyRelationships(obj: ObjectData)(implicit cassandraUtil: CassandraUtil, config: KnowlgPublishConfig): Unit = {
    try {
      val hierarchy = obj.hierarchy.getOrElse(Map.empty[String, AnyRef])
      if (hierarchy.nonEmpty) {
        val rootId = obj.identifier.replace(".img", "")
        
        // Convert Scala Map to Java Map for compatibility with original logic
        val javaHierarchy = ScalaJsonUtil.deserialize[java.util.Map[String, AnyRef]](ScalaJsonUtil.serialize(hierarchy))
        
        // Generate relationship data using exact logic from RelationCacheUpdater
        val leafNodesMap = getLeafNodes(rootId, javaHierarchy)
        val optionalNodesMap = getOptionalNodes(rootId, javaHierarchy)
        val ancestorsMap = getAncestors(rootId, javaHierarchy)
        
        // Store in Cassandra
        storeRelationshipData(rootId, "leafnodes", leafNodesMap)
        storeRelationshipData(rootId, "optionalnodes", optionalNodesMap)
        storeRelationshipData(rootId, "ancestors", ancestorsMap)
        
        logger.info(s"Hierarchy relationships updated for collection: $rootId - leafnodes: ${leafNodesMap.size}, optionalnodes: ${optionalNodesMap.size}, ancestors: ${ancestorsMap.size}")
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Error updating hierarchy relationships for ${obj.identifier}: ${ex.getMessage}", ex)
        // Don't fail the entire publish process for relationship storage errors
    }
  }

  // Exact logic from RelationCacheUpdater
  private def getLeafNodes(identifier: String, hierarchy: java.util.Map[String, AnyRef]): Map[String, List[String]] = {
    val mimeType = hierarchy.getOrDefault("mimeType", "").asInstanceOf[String]
    val leafNodesMap = if (StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection")) {
      val leafNodes = getOrComposeLeafNodes(hierarchy, false)
      val map: Map[String, List[String]] = if (leafNodes.nonEmpty) Map() + (identifier -> leafNodes) else Map()
      val children = getChildren(hierarchy)
      val childLeafNodesMap = if (CollectionUtils.isNotEmpty(children)) {
        children.asScala.map(child => {
          val childId = child.get("identifier").asInstanceOf[String]
          getLeafNodes(childId, child)
        }).flatten.toMap
      } else Map()
      map ++ childLeafNodesMap
    } else Map()
    leafNodesMap.filter(m => m._2.nonEmpty).toMap
  }

  private def getOrComposeLeafNodes(hierarchy: java.util.Map[String, AnyRef], compose: Boolean = true): List[String] = {
    if (hierarchy.containsKey("leafNodes") && !compose)
      hierarchy.getOrDefault("leafNodes", java.util.Arrays.asList()).asInstanceOf[java.util.List[String]].asScala.toList
    else {
      val children = getChildren(hierarchy)
      val childCollections = children.asScala.filter(c => isCollection(c))
      val leafList = childCollections.map(coll => getOrComposeLeafNodes(coll, true)).flatten.toList
      val ids = children.asScala.filterNot(c => isCollection(c)).map(c => c.getOrDefault("identifier", "").asInstanceOf[String]).filter(id => StringUtils.isNotBlank(id))
      leafList ++ ids
    }
  }

  private def isCollection(content: java.util.Map[String, AnyRef]): Boolean = {
    StringUtils.equalsIgnoreCase(content.getOrDefault("mimeType", "").asInstanceOf[String], "application/vnd.ekstep.content-collection")
  }

  private def getOptionalNodes(identifier: String, hierarchy: java.util.Map[String, AnyRef]): Map[String, List[String]] = {
    val mimeType = hierarchy.getOrDefault("mimeType", "").asInstanceOf[String]
    val optionalNodesMap = if (StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection")) {
      val optionalNodes = getOrComposeOptionalNodes(hierarchy, false)
      val map: Map[String, List[String]] = if (optionalNodes.nonEmpty) Map() + (identifier -> optionalNodes) else Map()
      val children = getChildren(hierarchy)
      val childOptionalNodesMap = if (CollectionUtils.isNotEmpty(children)) {
        children.asScala.map(child => {
          val childId = child.get("identifier").asInstanceOf[String]
          getOptionalNodes(childId, child)
        }).flatten.toMap
      } else Map()
      map ++ childOptionalNodesMap
    } else Map()
    optionalNodesMap.filter(m => m._2.nonEmpty).toMap
  }

  private def getOrComposeOptionalNodes(hierarchy: java.util.Map[String, AnyRef], compose: Boolean = true): List[String] = {
    val children = getChildren(hierarchy)
    val ids = children.asScala.filter(c => isOptional(c)).map(c => c.getOrDefault("identifier", "").asInstanceOf[String]).filter(id => StringUtils.isNotBlank(id))
    val childCollections = children.asScala.filterNot(c => isOptional(c))
    val optionalList = childCollections.map(coll => getOrComposeOptionalNodes(coll, true)).flatten.toList
    optionalList ++ ids
  }

  private def isOptional(content: java.util.Map[String, AnyRef]): Boolean = {
    val optionalMap = content.getOrDefault("relationalMetadata", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
    StringUtils.equalsIgnoreCase(optionalMap.getOrDefault("optional", "").toString, "true")
  }

  private def getAncestors(identifier: String, hierarchy: java.util.Map[String, AnyRef], parents: List[String] = List()): Map[String, List[String]] = {
    val mimeType = hierarchy.getOrDefault("mimeType", "").asInstanceOf[String]
    val isCollection = (StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection"))
    val ancestors = if (isCollection) identifier :: parents else parents
    val ancestorsMap = if (isCollection) {
      getChildren(hierarchy).asScala.map(child => {
        val childId = child.get("identifier").asInstanceOf[String]
        getAncestors(childId, child, ancestors)
      }).filter(m => m.nonEmpty).reduceOption((a, b) => {
        // Here we are merging the Resource ancestors where it is used multiple times - Functional.
        // convert maps to seq, to keep duplicate keys and concat then group by key - Code explanation.
        val grouped = (a.toSeq ++ b.toSeq).groupBy(_._1)
        grouped.mapValues(_.map(_._2).toList.flatten.distinct)
      }).getOrElse(Map())
    } else {
      Map(identifier -> parents)
    }
    ancestorsMap.filter(m => m._2.nonEmpty)
  }

  private def getChildren(hierarchy: java.util.Map[String, AnyRef]) = {
    val children = hierarchy.getOrDefault("children", java.util.Arrays.asList()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
    if (CollectionUtils.isEmpty(children)) List().asJava else children
  }

  /**
   * Stores relationship data in Cassandra/YugabyteDB using list<text>
   */
  private def storeRelationshipData(rootId: String, relationshipType: String, dataMap: Map[String, List[String]])
                                   (implicit cassandraUtil: CassandraUtil, config: KnowlgPublishConfig): Unit = {
    try {
      dataMap.foreach { case (identifier, nodeIds) =>
        val relationshipKey = if (StringUtils.isNotBlank(rootId)) s"${rootId}:${identifier}:${relationshipType}" else s"${identifier}:${relationshipType}"
        
        // Use Cassandra QueryBuilder for proper list<text> handling
        val insertQuery = QueryBuilder.insertInto(config.collectionHierarchyKeyspaceName, config.collectionHierarchyTableName)
        insertQuery.value("relationship_key", relationshipKey)
        insertQuery.value("node_ids", nodeIds.asJava) // Convert to Java List for Cassandra list<text>
        
        val result = cassandraUtil.upsert(insertQuery.toString)
        if (result) {
          logger.debug(s"Stored relationship data: $relationshipKey -> ${nodeIds.size} nodes")
        } else {
          logger.error(s"Failed to store relationship data for key: $relationshipKey")
        }
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Error storing relationship data for $rootId:$relationshipType: ${ex.getMessage}", ex)
        throw ex
    }
  }
}