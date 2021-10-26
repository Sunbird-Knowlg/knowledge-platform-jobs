package org.sunbird.job.content.publish.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Insert, QueryBuilder, Select}
import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.helpers._
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, ElasticSearchUtil, HttpUtil, JSONUtil, Neo4JUtil, ScalaJsonUtil, Slug}

import java.io.File
import java.io.IOException
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

trait CollectionPublisher extends ObjectReader with ObjectValidator with ObjectEnrichment with EcarGenerator with ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[CollectionPublisher])
  private val level4ContentTypes = List("Course", "CourseUnit", "LessonPlan", "LessonPlanUnit")
  private val EXPANDABLE_OBJECTS = List("Collection", "QuestionSet")
  private val EXCLUDE_LEAFNODE_OBJECTS = List("Collection", "Question")
  private val INCLUDE_LEAFNODE_OBJECTS = List("QuestionSet")
  private val INCLUDE_CHILDNODE_OBJECTS = List("Collection")
  private val PUBLISHED_STATUS_LIST = List("Live", "Unlisted")
  private val COLLECTION_MIME_TYPE = "application/vnd.ekstep.content-collection"
  private val mimeTypesToCheck = List("application/vnd.ekstep.h5p-archive", "application/vnd.ekstep.html-archive", "application/vnd.android.package-archive",
    "video/webm", "video/x-youtube", "video/mp4")

  override def getExtData(identifier: String, pkgVersion: Double, mimeType: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[ObjectExtData] = None

  override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
    val row: Row = Option(getCollectionHierarchy(getEditableObjId(identifier, pkgVersion), readerConfig)).getOrElse(getCollectionHierarchy(identifier, readerConfig))
    if (null != row) {
      val data: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](row.getString("hierarchy"))
      Option(data)
    } else Option(Map.empty[String, AnyRef])
  }

  def getCollectionHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Row = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(readerConfig.keyspace, readerConfig.table).
      where()
    selectWhere.and(QueryBuilder.eq("identifier", identifier))
    cassandraUtil.findOne(selectWhere.toString)
  }

  override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def enrichObjectMetadata(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig): Option[ObjectData] = {
    val contentConfig = config.asInstanceOf[ContentPublishConfig]
    val extraMeta = Map("pkgVersion" -> (obj.pkgVersion + 1).asInstanceOf[AnyRef], "lastPublishedOn" -> getTimeStamp,
      "flagReasons" -> null, "body" -> null, "publishError" -> null, "variants" -> null, "downloadUrl" -> null)
    val contentSize = obj.metadata.getOrElse("size", 0).toString.toDouble
    val configSize = contentConfig.artifactSizeForOnline
    val updatedMeta: Map[String, AnyRef] = if (contentSize > configSize) obj.metadata ++ extraMeta ++ Map("contentDisposition" -> "online-only") else obj.metadata ++ extraMeta

    val updatedCompatibilityLevelMeta: Map[String, AnyRef] = setCompatibilityLevel(obj, updatedMeta).get

    val isCollectionShallowCopy =  isContentShallowCopy(obj)

    // Collection - Enrich Children - line 345
    val collectionHierarchy: Map[String, AnyRef] = if (isCollectionShallowCopy) {
      val originData: Map[String, AnyRef] = obj.metadata.getOrElse("originData","").asInstanceOf[Map[String,AnyRef]]
      getHierarchy(obj.metadata.get("origin").asInstanceOf[String], originData.getOrElse("pkgVersion", 0.0).asInstanceOf[Double], readerConfig).get
    } else getHierarchy(obj.identifier, obj.pkgVersion, readerConfig).get
    println("Hierarchy for content : " + obj.identifier + " : " + collectionHierarchy)

    val children = if (collectionHierarchy.nonEmpty) { collectionHierarchy.getOrElse("children",List.empty[Map[String, AnyRef]]).asInstanceOf[List[Map[String, AnyRef]]] } else List.empty[Map[String, AnyRef]]
    val childrenBuffer = children.to[ListBuffer]
    val updatedObjMetadata: Map[String,AnyRef] = if (collectionHierarchy.nonEmpty && !isCollectionShallowCopy) {
        val collectionResourceChildNodes: mutable.HashSet[String] = mutable.HashSet.empty[String]
        val toEnrichChildrenObj = new ObjectData(obj.identifier, updatedCompatibilityLevelMeta, obj.extData, Option(collectionHierarchy))
        val enrichedChildrenObject = enrichChildren(childrenBuffer, collectionResourceChildNodes, toEnrichChildrenObj)
        if (collectionResourceChildNodes.nonEmpty) {
          val collectionChildNodes: List[String] = enrichedChildrenObject.metadata.getOrElse("childNodes", new java.util.ArrayList()).asInstanceOf[java.util.List[String]].asScala.toList
          updatedCompatibilityLevelMeta ++ Map("childNodes" -> (collectionChildNodes ++ collectionResourceChildNodes).distinct)
        } else enrichedChildrenObject.metadata
    } else updatedCompatibilityLevelMeta

    val updatedObj = new ObjectData(obj.identifier, updatedObjMetadata, obj.extData, Option(collectionHierarchy))

    logger.info("Collection processing started for content: " + updatedObj.identifier)
    val enrichedObj = processCollection(updatedObj, childrenBuffer.toList)
    logger.info("Collection processing done for content: " + enrichedObj.identifier)
    logger.info("Collection data after processing for : " + enrichedObj.identifier + " | Metadata : " + enrichedObj.metadata)
    logger.info("Collection children data after processing : " + enrichedObj.hierarchy.get("children"))

    Some(enrichedObj)
  }

  override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = {
    Some(List(obj.metadata ++ obj.extData.getOrElse(Map()).filter(p => !excludeBundleMeta.contains(p._1))))
  }

  override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

  override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

//  def validateMetadata(obj: ObjectData, identifier: String): List[String] = {
//    logger.info("Validating Collection metadata for : " + obj.identifier)
//    val messages = ListBuffer[String]()
//   if (StringUtils.isBlank(obj.getString("artifactUrl", "")))
//      messages += s"""There is no artifactUrl available for : $identifier"""
//
//    messages.toList
//  }

  def getObjectWithEcar(data: ObjectData, pkgTypes: List[String])(implicit ec: ExecutionContext, neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig, httpUtil: HttpUtil): ObjectData = {

    // Line 1107 in PublishFinalizer
    val children = data.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
    val updatedChildren = updateHierarchyMetadata(children, data)(config)
    val updatedObj = updateRootChildrenList(data, updatedChildren)
    val nodes = ListBuffer.empty[ObjectData]
    val nodeIds = ListBuffer.empty[String]
    nodes += data
    nodeIds += data.identifier
    getNodeMap(children, nodes, nodeIds)

    logger.info("CollectionPulisher:getObjectWithEcar: Ecar generation done for Content: " + updatedObj.identifier)
    val ecarMap: Map[String, String] = generateEcar(updatedObj, pkgTypes)
    val variants: java.util.Map[String, java.util.Map[String, String]] = ecarMap.map { case (key, value) => key.toLowerCase -> Map[String, String]("ecarUrl" -> value, "size" -> httpUtil.getSize(value).toString).asJava }.asJava
    logger.info("CollectionPulisher ::: getObjectWithEcar ::: ecar map ::: " + ecarMap)
    val meta: Map[String, AnyRef] = Map("downloadUrl" -> ecarMap.getOrElse(EcarPackageType.SPINE.toString, ""), "variants" -> variants)
    new ObjectData(updatedObj.identifier, updatedObj.metadata ++ meta, updatedObj.extData, updatedObj.hierarchy)
  }

  private def setCompatibilityLevel(obj: ObjectData, updatedMeta: Map[String, AnyRef]): Option[Map[String, AnyRef]] = {
    if (level4ContentTypes.contains(obj.getString("contentType", ""))) {
      logger.info("setting compatibility level for content id : " + obj.identifier + " as 4.")
      Some(updatedMeta ++ Map("compatibilityLevel" -> 4.asInstanceOf[AnyRef]))
    } else Some(updatedMeta)
  }

  def getUnitsFromLiveContent(obj: ObjectData)(implicit cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): List[String] = {
    val objHierarchy = getHierarchy(obj.metadata.getOrElse("identifier", "").asInstanceOf[String], obj.metadata.getOrElse("pkgVersion", 0.0).asInstanceOf[Double], readerConfig).get
    val children = objHierarchy.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]]
    if(children.nonEmpty) {
      children.map(child => {
        if(child.getOrElse("visibility", "").asInstanceOf[String].equalsIgnoreCase("Parent")){
          child.getOrElse("identifier", "").asInstanceOf[String]
        } else ""
      }).filter(rec => rec.nonEmpty)
    } else List.empty[String]
  }

  def isContentShallowCopy(obj: ObjectData): Boolean = {
    val originData: Map[String, AnyRef] = obj.metadata.getOrElse("originData",Map.empty[String, AnyRef]).asInstanceOf[Map[String,AnyRef]]
    if (originData != null && originData.nonEmpty && StringUtils.isNoneBlank(originData.getOrElse("copyType","").asInstanceOf[String]) && StringUtils.equalsIgnoreCase(originData.getOrElse("copyType","").asInstanceOf[String], "shallow")) true
    else false
  }

  def updateOriginPkgVersion(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil): ObjectData = {
    val originId = obj.metadata.getOrElse("origin", "").asInstanceOf[String]
    val originNodeMetadata = Option(neo4JUtil.getNodeProperties(originId)).getOrElse(neo4JUtil.getNodeProperties(originId))
    if (null != originNodeMetadata) {
      val originPkgVer: Double = originNodeMetadata.getOrDefault("pkgVersion", "0").asInstanceOf[Any].asInstanceOf[Double]
      if (originPkgVer != 0.0) {
        val originData = obj.metadata.getOrElse("originData",Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]] ++ Map("pkgVersion" -> originPkgVer)
        new ObjectData(obj.identifier, obj.metadata ++ Map("originData" -> originData) , obj.extData, obj.hierarchy)
      } else obj
    } else obj
  }

  private def enrichChildren(children: ListBuffer[Map[String, AnyRef]], collectionResourceChildNodes: mutable.HashSet[String], obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): ObjectData = {
    if (children.nonEmpty) {
      val newChildren = children.toList
      val childNodesToRemove: List[String] = newChildren.map(child => {
        if (StringUtils.equalsIgnoreCase(child.getOrElse("visibility","").asInstanceOf[String], "Parent") && StringUtils.equalsIgnoreCase(child.getOrElse("mimeType","").asInstanceOf[String], COLLECTION_MIME_TYPE))
          enrichChildren(child.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]].to[ListBuffer], collectionResourceChildNodes, obj)

        if (StringUtils.equalsIgnoreCase(child.getOrElse("visibility", "").asInstanceOf[String], "Default") && EXPANDABLE_OBJECTS.contains(child.getOrElse("objectType", "").asInstanceOf[String])) {
          val collectionHierarchy = getHierarchy(child.getOrElse("identifier","").asInstanceOf[String], child.getOrElse("pkgVersion", 0.0).asInstanceOf[Double], readerConfig).get
          logger.debug("Collection hierarchy for childNode : " + child.getOrElse("identifier","") + " : " + collectionHierarchy)
          if (collectionHierarchy.nonEmpty) {
            val childNodes = collectionHierarchy.getOrElse("childNodes", List.empty).asInstanceOf[List[String]]
            if (childNodes.nonEmpty && INCLUDE_CHILDNODE_OBJECTS.contains(child.getOrElse("objectType","").asInstanceOf[String])) collectionResourceChildNodes ++= childNodes.toSet[String]
            children.remove(children.indexOf(child))
            children ++= ListBuffer(collectionHierarchy ++ Map("index" -> child.getOrElse("index",0).asInstanceOf[AnyRef], "parent" -> child.getOrElse("parent","")))
          }
        }

        if (StringUtils.equalsIgnoreCase(child.getOrElse("visibility", "").asInstanceOf[String], "Default") && !EXPANDABLE_OBJECTS.contains(child.getOrElse("objectType", "").asInstanceOf[String])) {
          val childNode = Option(neo4JUtil.getNodeProperties(child.getOrElse("identifier", "").asInstanceOf[String])).getOrElse(neo4JUtil.getNodeProperties(child.getOrElse("identifier", "").asInstanceOf[String])).asScala.toMap
          children.remove(children.indexOf(child))

          if (PUBLISHED_STATUS_LIST.contains(childNode.getOrElse("status", "").asInstanceOf[String])) {
            children ++= ListBuffer(childNode ++ Map("index" -> child.getOrElse("index",0).asInstanceOf[AnyRef], "parent" -> child.getOrElse("parent","").asInstanceOf[String], "depth" -> child.getOrElse("depth",0).asInstanceOf[AnyRef]) - ("collections", "children"))
          ""
          } else child.getOrElse("identifier", "").asInstanceOf[String]
        } else ""
      }).filter(rec => rec.nonEmpty)

      if(childNodesToRemove.nonEmpty) {
        val originalChildNodes = obj.metadata.getOrElse("childNodes", new java.util.ArrayList()).asInstanceOf[java.util.List[String]].asScala.toList
        new ObjectData(obj.identifier, obj.metadata ++ Map("childNodes" -> originalChildNodes.filter(rec => !childNodesToRemove.contains(rec))), obj.extData, obj.hierarchy)
      } else obj
    } else obj
  }


  private def processCollection(obj: ObjectData, children: List[Map[String, AnyRef]])(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig): ObjectData = {
    val contentId = obj.identifier
    val dataMap: mutable.Map[String, AnyRef] = processChildren(obj, children)
    logger.info("Children nodes process for collection - " + contentId)
    val updatedObj: ObjectData = if (dataMap.nonEmpty) {
     val updatedMetadataMap: Map[String, AnyRef] = dataMap.flatMap(record => {
        if (!"concepts".equalsIgnoreCase(record._1) && !"keywords".equalsIgnoreCase(record._1)) {
          Map(record._1 -> record._2.asInstanceOf[Set[String]].toArray[String])
        } else Map.empty[String, AnyRef]
      }).filter(record => record._1.nonEmpty).toMap[String, AnyRef]
      val keywords = dataMap.getOrElse("keywords", Set.empty).asInstanceOf[Set[String]].toArray[String]
      val finalKeywords = if (null != keywords && keywords.nonEmpty) {
       val updatedKeywords: Array[String] = if (null != obj.metadata.get("keywords")) {
          val objKeywords = obj.metadata.get("keywords")
          if (objKeywords.isInstanceOf[Array[String]]) {
            val stringArray = obj.metadata.getOrElse("keywords",Array.empty).asInstanceOf[Array[String]]
            keywords ++ stringArray
          }
          else if (objKeywords.isInstanceOf[String]) {
            keywords ++ Array[String](objKeywords.asInstanceOf[String])
          }
          else Array.empty[String]
        } else Array.empty[String]
       updatedKeywords.filter(record => record.trim.nonEmpty).distinct
      } else Array.empty[String]

      new ObjectData(obj.identifier, (obj.metadata ++ Map("keywords" -> finalKeywords.asInstanceOf[AnyRef]) ++ updatedMetadataMap), obj.extData, obj.hierarchy)
    } else obj

    val enrichedObject = enrichCollection(updatedObj, children)
//    addResourceToCollection(enrichedObject, children.to[ListBuffer]) - TO DO
    enrichedObject

  }

//  private def addResourceToCollection(obj: ObjectData, children: ListBuffer[Map[String, AnyRef]]): Unit = {
//    val leafNodes = getLeafNodes(children, 1)
//    if (leafNodes.nonEmpty) {
//      val relations = new ArrayList[Relation]
//      for (leafNode <- leafNodes) {
//        val id = leafNode.getOrElse("identifier", "").asInstanceOf[String]
//        var index = 1
//        val num = leafNode.getOrElse("index",0).asInstanceOf[AnyRef].asInstanceOf[Number]
//        if (num != null) index = num.intValue
//        val rel = new Relation(node.getIdentifier, RelationTypes.SEQUENCE_MEMBERSHIP.relationName, id)
//        val metadata = new HashMap[String, AnyRef]
//        metadata.put(SystemProperties.IL_SEQUENCE_INDEX", index)
//        metadata.put("depth", leafNode.getOrElse("depth",0).asInstanceOf[AnyRef])
//        rel.setMetadata(metadata)
//        relations.add(rel)
//      }
//      val existingRelations = node.getOutRelations
//      if (CollectionUtils.isNotEmpty(existingRelations)) relations.addAll(existingRelations)
//      node.setOutRelations(relations)
//    }
//  }

  private def getLeafNodes(children: ListBuffer[Map[String, AnyRef]], depth: Int): List[Map[String, AnyRef]] = {
    val leafNodes = new ListBuffer[Map[String, AnyRef]]
    if (children.nonEmpty) {
      var index = 1
      for (child <- children) {
        val visibility = child.getOrElse("visibility", "").asInstanceOf[String]
        if (StringUtils.equalsIgnoreCase(visibility, "Parent")) {
          val nextChildren = child.getOrElse("children", ListBuffer.empty).asInstanceOf[ListBuffer[Map[String, AnyRef]]]
          val nextDepth = depth + 1
          val nextLevelLeafNodes = getLeafNodes(nextChildren, nextDepth)
          leafNodes ++= nextLevelLeafNodes
        }
        else {
          leafNodes ++ (child ++ Map ("index" -> index, "depth"-> depth))
          index += 1
        }
      }
    }
    leafNodes.toList
  }

  private def processChildren(obj: ObjectData, children: List[Map[String, AnyRef]]): mutable.Map[String, AnyRef] = {
    val dataMap: mutable.Map[String, AnyRef] = mutable.Map.empty
    processChildren(children, dataMap)
    dataMap
  }

  private def processChildren(children: List[Map[String, AnyRef]], dataMap: mutable.Map[String, AnyRef]): Unit = {
    if (null != children && children.nonEmpty) {
      for (child <- children) {
        mergeMap(dataMap, processChild(child))
        processChildren(child.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]], dataMap)
      }
    }
  }

  def enrichCollection(obj: ObjectData, children: List[Map[String, AnyRef]])(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig): ObjectData = {
    val nodeMetadata= mutable.Map.empty[String, AnyRef] ++ obj.metadata
    val contentId = obj.identifier
    logger.info("Processing Collection Content :" + contentId)
    if (null != children && children.nonEmpty) {
      val content = getHierarchy(obj.identifier, obj.pkgVersion, readerConfig).get
      if (content.isEmpty) return obj
      val leafCount = getLeafNodeCount(content)
      val totalCompressedSize = getTotalCompressedSize(content, 0.0)

      nodeMetadata.put("leafNodesCount", leafCount.asInstanceOf[AnyRef])
      nodeMetadata.put("totalCompressedSize", totalCompressedSize.asInstanceOf[AnyRef])

      nodeMetadata.put("leafNodes",updateLeafNodeIds(obj, content))
      val mimeTypeMap: mutable.Map[String, AnyRef] = mutable.Map.empty[String, AnyRef]
      val contentTypeMap: mutable.Map[String, AnyRef] = mutable.Map.empty[String, AnyRef]
      getTypeCount(content, "mimeType", mimeTypeMap)
      getTypeCount(content, "contentType", contentTypeMap)

      val updatedContent = content ++ Map("leafNodesCount"-> leafCount, "totalCompressedSize"-> totalCompressedSize,"mimeTypesCount"-> mimeTypeMap,"contentTypesCount"-> contentTypeMap).asInstanceOf[Map[String, AnyRef]]
      nodeMetadata.put("mimeTypesCount", mimeTypeMap)
      nodeMetadata.put("contentTypesCount", contentTypeMap)
      nodeMetadata.put("toc_url", generateTOC(obj, updatedContent).asInstanceOf[AnyRef])

      val updatedMetadata: Map[String, AnyRef] =  try {
        nodeMetadata.put("mimeTypesCount", JSONUtil.serialize(mimeTypeMap))
        nodeMetadata.put("contentTypesCount", JSONUtil.serialize(contentTypeMap))
        setContentAndCategoryTypes(nodeMetadata.toMap)
      } catch {
        case e: Exception =>  logger.error("Error while stringify mimeTypeCount or contentTypesCount:", e)
          nodeMetadata.toMap
      }

      new ObjectData(obj.identifier, updatedMetadata, obj.extData, Option(updatedContent))
    } else obj
  }

  private def updateLeafNodeIds(obj: ObjectData, content: Map[String, AnyRef]): Array[String] = {
    val leafNodeIds: mutable.Set[String] = mutable.Set.empty[String]
    getLeafNodesIds(content, leafNodeIds)
    leafNodeIds.toArray
  }

  private def processChild(childMetadata: Map[String, AnyRef]): Map[String, AnyRef] = {
    val taggingProperties = List("language", "domain", "ageGroup", "genre", "theme", "keywords")
    val result: Map[String, AnyRef] = childMetadata.flatMap(prop => {
      if (taggingProperties.contains(prop._1)) {
        val o = childMetadata.get(prop._1)
        if (o.isInstanceOf[String]) Map(prop._1 -> Set(o.asInstanceOf[String]))
        else if (o.isInstanceOf[List[_]]) Map(prop._1 -> o.asInstanceOf[Set[String]])
        else Map.empty[String, AnyRef]
      } else Map.empty[String, AnyRef]
    }).filter(rec => rec._1.nonEmpty)
    result
  }

  private def mergeMap(dataMap: mutable.Map[String, AnyRef], childDataMap: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    if (dataMap.isEmpty) dataMap ++= childDataMap
    else {
      dataMap.map(record => {
        dataMap += (record._1 -> (if (childDataMap.contains(record._1)) (childDataMap.get(record._1).asInstanceOf[mutable.Set[String]] ++ record._2.asInstanceOf[mutable.Set[String]]) else record._2.asInstanceOf[mutable.Set[String]]))
      })
      if (!dataMap.equals(childDataMap)) {
        childDataMap.map(record => {
          if (!dataMap.contains(record._1)) dataMap += record
        })
      }
    }
    dataMap
  }

//
//  private def getChildNode(data: Map[String, AnyRef], childrenSet: scala.collection.mutable.SortedSet[String]): Unit = {
//    val children = data.get("children").asInstanceOf[List[AnyRef]]
//      if (null != children && children.nonEmpty) {
//      children.map(child => {
//        val childMap = child.asInstanceOf[Map[String, AnyRef]]
//        childMap.getOrElse("identifier", "").asInstanceOf[String]
//        getChildNode(childMap, childrenSet)
//      })
//    }
//  }

  private def getTypeCount(data: Map[String, AnyRef], `type`: String, typeMap: mutable.Map[String, AnyRef]): Unit = {
    val children = data.getOrElse("children", List.empty).asInstanceOf[List[AnyRef]]
    if (null != children && children.nonEmpty) {
      for (child <- children) {
        val childMap = child.asInstanceOf[Map[String, AnyRef]]
        val typeValue = childMap.getOrElse(`type`, "").asInstanceOf[String]
        if (null != typeValue) if (typeMap.contains(typeValue)) {
          var count = typeMap.getOrElse(typeValue, 0).asInstanceOf[Int]
          count += 1
          typeMap.put(typeValue, count.asInstanceOf[AnyRef])
        }
        else typeMap.put(typeValue, 1.asInstanceOf[AnyRef])
        if (childMap.contains("children")) getTypeCount(childMap, `type`, typeMap)
      }
    }
  }

  @SuppressWarnings(Array("unchecked"))
  private def getLeafNodeCount(data: Map[String, AnyRef]): Int = {
    val leafNodeIds: mutable.Set[String] = mutable.Set.empty[String]
    getLeafNodesIds(data,leafNodeIds)
    leafNodeIds.size
  }

  private def getLeafNodesIds(data: Map[String, AnyRef], leafNodeIds: mutable.Set[String]): Unit = {
    if (INCLUDE_LEAFNODE_OBJECTS.contains(data.getOrElse("objectType", ""))) leafNodeIds += (data.getOrElse("identifier", "").asInstanceOf[String])
    val children = data.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]]
    if (children.nonEmpty) {
      for (child <- children) {
        getLeafNodesIds(child, leafNodeIds)
      }
    }
    else if (!EXCLUDE_LEAFNODE_OBJECTS.contains(data.getOrElse("objectType", "").asInstanceOf[String])) leafNodeIds.add(data.getOrElse("identifier", "").asInstanceOf[String])
  }

  private def getTotalCompressedSize(data: Map[String, AnyRef], totalCompressed: Double): Double = {
    val children = data.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]]
    if (children.nonEmpty) {
      val childrenSizes = children.map(child => {
        val childSize =
          if (!EXPANDABLE_OBJECTS.contains(child.getOrElse("objectType", "").asInstanceOf[String]) && StringUtils.equals(child.getOrElse("visibility", "").asInstanceOf[String], "Default")) {
            if (null != child.get("totalCompressedSize")) child.getOrElse("totalCompressedSize",0).asInstanceOf[Number].doubleValue
            else if (null != child.get("size")) child.getOrElse("size",0).asInstanceOf[Number].doubleValue
            else 0
          } else 0

        getTotalCompressedSize(child, childSize)
      }).sum
      totalCompressed + childrenSizes
    }
    else totalCompressed
  }

  def generateTOC(obj: ObjectData, content: Map[String, AnyRef])(implicit cloudStorageUtil: CloudStorageUtil, config: PublishConfig): String = {
    logger.info("Write hierarchy to JSON File :" +obj.identifier)
    val file = new File(getTOCBasePath(obj.identifier) + "_toc.json")
    try {
      val data = ScalaJsonUtil.serialize(content)
      FileUtils.writeStringToFile(file, data, "UTF-8")
      val url: String = if (file.exists) {
        logger.info("Upload File to cloud storage :" + file.getName)
        val uploadedFileUrl = cloudStorageUtil.uploadFile(getAWSPath(obj.identifier), file, Option.apply(true))
        if (null != uploadedFileUrl && uploadedFileUrl.length > 1) {
          logger.info("Update cloud storage url to node" + uploadedFileUrl(1))
          uploadedFileUrl(1)
        } else ""
      } else ""
      url
    } catch {
      case e: JsonProcessingException =>  logger.error("Error while parsing map object to string.", e)
        throw e
      case e: Exception =>  logger.error("Error while uploading file ", e)
        throw e
    } finally try {
      logger.info("Deleting Uploaded files")
      FileUtils.deleteDirectory(file.getParentFile)
    } catch {
      case e: IOException =>
        logger.error("Error while deleting file ", e)
    }
  }

  private def getTOCBasePath(contentId: String)(implicit cloudStorageUtil: CloudStorageUtil, config: PublishConfig): String = {
    if (contentId.nonEmpty) "/tmp" + File.separator + System.currentTimeMillis + "_temp" + File.separator + contentId else ""
  }

  private def getAWSPath(identifier: String)(implicit cloudStorageUtil: CloudStorageUtil, config: PublishConfig): String = {
    val contentConfig = config.asInstanceOf[ContentPublishConfig]
    val folderName = contentConfig.contentFolder
    if (folderName.nonEmpty) folderName + File.separator + Slug.makeSlug(identifier, isTransliterate = true) + File.separator + contentConfig.artifactFolder else folderName
  }

  def setContentAndCategoryTypes(input: Map[String, AnyRef])(implicit config: PublishConfig): Map[String, AnyRef] = {
    val contentConfig = config.asInstanceOf[ContentPublishConfig]
    val categoryMap = contentConfig.categoryMap
    val categoryMapForMimeType = contentConfig.categoryMapForMimeType
    val categoryMapForResourceType = contentConfig.categoryMapForResourceType
      val contentType = input.getOrElse("contentType", "").asInstanceOf[String]
      val primaryCategory = input.getOrElse("primaryCategory","").asInstanceOf[String]
      val (updatedContentType, updatedPrimaryCategory): (String, String) = (contentType, primaryCategory) match {
        case (x: String, y: String) => (x, y)
        case ("Resource", y) => (contentType, getCategoryForResource(input.getOrElse("mimeType", "").asInstanceOf[String],
          input.getOrElse("resourceType", "").asInstanceOf[String], categoryMapForMimeType, categoryMapForResourceType))
        case (x: String, y) => (x, categoryMap.getOrDefault(x,"").asInstanceOf[String])
        case (x, y: String) => (categoryMap.asScala.filter(entry => StringUtils.equalsIgnoreCase(entry._2.asInstanceOf[String], y)).keys.headOption.getOrElse(""), y)
        case _ => (contentType, primaryCategory)
      }

      input ++ Map("contentType" -> updatedContentType, "primaryCategory" -> updatedPrimaryCategory)
  }

  private def getCategoryForResource(mimeType: String, resourceType: String, categoryMapForMimeType: java.util.Map[String, AnyRef], categoryMapForResourceType: java.util.Map[String, AnyRef]): String = (mimeType, resourceType) match {
    case ("", "") => "Learning Resource"
    case (x: String, "") => categoryMapForMimeType.get(x).asInstanceOf[java.util.List[String]].asScala.headOption.getOrElse("Learning Resource")
    case (x: String, y: String) => if (mimeTypesToCheck.contains(x)) categoryMapForMimeType.get(x).asInstanceOf[java.util.List[String]].asScala.headOption.getOrElse("Learning Resource") else categoryMapForResourceType.getOrDefault(y, "Learning Resource").asInstanceOf[String]
    case _ => "Learning Resource"
  }


  def updateHierarchyMetadata(children: List[Map[String, AnyRef]], obj: ObjectData)(implicit config: PublishConfig): List[Map[String, AnyRef]] = {
   if (children.nonEmpty) {
      children.map(child => {
        if (StringUtils.equalsIgnoreCase("Parent", child.getOrElse("visibility", "").asInstanceOf[String])) { //set child metadata -- compatibilityLevel, appIcon, posterImage, lastPublishedOn, pkgVersion, status
          val updatedChild = populatePublishMetadata(child, obj)
          updateHierarchyMetadata(updatedChild.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]], obj)
          updatedChild
        } else child
      })
    } else children
  }

  private def populatePublishMetadata(content: Map[String, AnyRef], obj: ObjectData)(implicit config: PublishConfig): Map[String, AnyRef] = {
    //TODO:  For appIcon, posterImage and screenshot createThumbNail method has to be implemented.
    val leafNodeIds: mutable.Set[String] = mutable.Set.empty[String]
    getLeafNodesIds(content, leafNodeIds)

    val updatedContent = content ++
    Map("compatibilityLevel" -> (if (null != content.get("compatibilityLevel")) content.getOrElse("compatibilityLevel",1).asInstanceOf[Number].intValue else 1),
    "lastPublishedOn" -> obj.metadata.get("lastPublishedOn"), "pkgVersion" -> obj.metadata.get("pkgVersion"), "leafNodesCount" -> getLeafNodeCount(content),
    "leafNodes" -> leafNodeIds.toArray[String], "status" -> obj.metadata.get("status"), "lastUpdatedOn" -> obj.metadata.get("lastUpdatedOn"),
      "downloadUrl"-> obj.metadata.get("downloadUrl"), "variants" -> obj.metadata.get("variants")).asInstanceOf[Map[String, AnyRef]]

    // PRIMARY CATEGORY MAPPING IS DONE
    setContentAndCategoryTypes(updatedContent)
  }

  def publishHierarchy(children: List[Map[String, AnyRef]], obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val identifier = obj.identifier.replace(".img", "")
//    val children: List[Map[String, AnyRef]] = obj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
    val hierarchy: Map[String, AnyRef] = obj.metadata ++ Map("children" -> children)
    val data = Map("hierarchy" -> hierarchy) ++ obj.extData.getOrElse(Map())
    val query: Insert = QueryBuilder.insertInto(readerConfig.keyspace, readerConfig.table)
    query.value(readerConfig.primaryKey.head, identifier)
    data.map(d => {
      readerConfig.propsMapping.getOrElse(d._1, "") match {
        case "blob" => query.value(d._1.toLowerCase, QueryBuilder.fcall("textAsBlob", d._2))
        case "string" => d._2 match {
          case value: String => query.value(d._1.toLowerCase, value)
          case _ => query.value(d._1.toLowerCase, JSONUtil.serialize(d._2))
        }
        case _ => query.value(d._1, d._2)
      }
    })
    logger.debug(s"Publishing Hierarchy data for $identifier | Query : ${query.toString}")
    val result = cassandraUtil.upsert(query.toString)
    if (result) {
      logger.info(s"Hierarchy data saved successfully for ${identifier}")
    } else {
      val msg = s"Hierarchy Data Insertion Failed For ${identifier}"
      logger.error(msg)
      throw new Exception(msg)
    }

  }

  private def updateRootChildrenList(obj: ObjectData, nextLevelNodes: List[Map[String, AnyRef]]): ObjectData = {
    val childrenMap: List[Map[String, AnyRef]] =
      nextLevelNodes.map(record => {
        Map("identifier" -> record.getOrElse("identifier", "").asInstanceOf[String],
            "name" -> record.getOrElse("name","").asInstanceOf[String],
            "objectType" -> record.getOrElse("objectType", "").asInstanceOf[String],
            "description" -> record.getOrElse("description","").asInstanceOf[String],
            "index" -> record.getOrElse("index",0).asInstanceOf[AnyRef].asInstanceOf[AnyRef])
      })

    new ObjectData(obj.identifier, obj.metadata ++ Map("children"-> childrenMap), obj.extData, obj.hierarchy)
  }

  private def getNodeMap(children: List[Map[String, AnyRef]], nodes: ListBuffer[ObjectData], nodeIds: ListBuffer[String])(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): Unit = {
    if (children.nonEmpty) {
      children.foreach((child: Map[String, AnyRef]) => {
         try {
           val updatedChildMetadata: Map[String, AnyRef] = if (StringUtils.equalsIgnoreCase("Default", child.getOrElse("visibility", "").asInstanceOf[String])) {
             val nodeMetadata = neo4JUtil.getNodeProperties(child.getOrElse("identifier", "").asInstanceOf[String]) // CHECK IF THIS IS GOOD
              nodeMetadata.remove("children")
//              val childData: mutable.Map[String, AnyRef] = mutable.Map.empty[String, AnyRef]
//              childData += child
              val nextLevelNodes: List[Map[String, AnyRef]] = child.getOrElse("children",List.empty).asInstanceOf[List[Map[String, AnyRef]]]
              val finalChildList = if (nextLevelNodes.nonEmpty) {
                nextLevelNodes.map((nextLevelNode: Map[String, AnyRef]) => {
                  Map("identifier" -> nextLevelNode.getOrElse("identifier", "").asInstanceOf[String], "name" -> nextLevelNode.getOrElse("name","").asInstanceOf[String],
                    "objectType" -> nextLevelNode.getOrElse("objectType", "").asInstanceOf[String], "description" -> nextLevelNode.getOrElse("description","").asInstanceOf[String],
                    "index" -> nextLevelNode.getOrElse("index",0).asInstanceOf[AnyRef].asInstanceOf[String])
                })
              }
              nodeMetadata.put("children", finalChildList.asInstanceOf[AnyRef])
             nodeMetadata.asScala.toMap[String, AnyRef]
            }
            else {
//              val childData: mutable.Map[String, AnyRef] = mutable.Map.empty[String, AnyRef]
//              childData += child
//              childData.remove("children")
              val nextLevelNodes: List[Map[String, AnyRef]] = child.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]]
              val nodeMetadata: mutable.Map[String, AnyRef] = mutable.Map() ++ getHierarchy(child.getOrElse("identifier", "").asInstanceOf[String], child.getOrElse("pkgVersion",1).asInstanceOf[Int], readerConfig).get // CHECK WHAT VALUE IS TO BE PUT HERE
              val finalChildList = if (nextLevelNodes.nonEmpty) {
                nextLevelNodes.map((nextLevelNode: Map[String, AnyRef]) => {
                  Map("identifier" -> nextLevelNode.getOrElse("identifier", "").asInstanceOf[String], "name" -> nextLevelNode.getOrElse("name","").asInstanceOf[String],
                    "objectType" -> nextLevelNode.getOrElse("objectType", "").asInstanceOf[String], "description" -> nextLevelNode.getOrElse("description","").asInstanceOf[String],
                    "index" -> nextLevelNode.getOrElse("index",0).asInstanceOf[AnyRef].asInstanceOf[String])
                })
              }
              nodeMetadata.put("children", finalChildList.asInstanceOf[AnyRef])
              if (nodeMetadata.getOrElse("objectType", "").asInstanceOf[String].isEmpty) {
                nodeMetadata += ("objectType" -> "content")
              }
              if (nodeMetadata.getOrElse("graphId", "").asInstanceOf[String].isEmpty) {
                nodeMetadata += ("graphId" -> "domain")
              }
             nodeMetadata.toMap[String, AnyRef]
            }
            if (!nodeIds.contains(child.getOrElse("identifier", "").asInstanceOf[String])) {
              nodes += new ObjectData(child.getOrElse("identifier", "").asInstanceOf[String], updatedChildMetadata, Option(Map.empty[String, AnyRef]), Option(Map.empty[String, AnyRef]))
              nodeIds += child.getOrElse("identifier", "").asInstanceOf[String]
            }
          } catch {
            case e: Exception => logger.error("Error while generating node map. ", e)
          }
          getNodeMap(child.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]], nodes, nodeIds)
      })
    }
  }

//  private def syncNodes(children: List[Map[String, AnyRef]], unitNodes: List[String])(implicit esUtil: ElasticSearchUtil, neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): Unit = {
//    val nodes = ListBuffer.empty[ObjectData]
//    val nodeIds = ListBuffer.empty[String]
//
//    getNodeForSyncing(children, nodes, nodeIds)
//    val updatedUnitNodes = if (unitNodes.nonEmpty) unitNodes.filter(unitNode => !nodeIds.contains(unitNode)) else unitNodes
//
//    if (nodes.isEmpty && updatedUnitNodes.isEmpty ) return
//
//    var errors = null
//    val `def` = mapper.convertValue(definition, new TypeReference[Map[String, AnyRef]]() {})
//    val relationMap = GraphUtil.getRelationMap(ContentWorkflowPipelineParams.Collection.name, `def`)
//    if (CollectionUtils.isNotEmpty(nodes)) while ( {
//      !nodes.isEmpty
//    }) {
//      val currentBatchSize = if (nodes.size >= batchSize) batchSize
//      else nodes.size
//      val nodeBatch = nodes.subList(0, currentBatchSize)
//      if (CollectionUtils.isNotEmpty(nodeBatch)) {
//        errors = new HashMap[String, String]
//        val messages = SyncMessageGenerator.getMessages(nodeBatch, ContentWorkflowPipelineParams.Collection.name, relationMap, errors, disableAkka)
//        if (!errors.isEmpty) logger.error("Error! while forming ES document data from nodes, below nodes are ignored: " + errors)
//        if (MapUtils.isNotEmpty(messages)) try {
//          System.out.println("Number of units to be synced : " + messages.size)
//          ElasticSearchUtil.bulkIndexWithIndexId(ES_INDEX_NAME, DOCUMENT_TYPE, messages)
//          System.out.println("UnitIds synced : " + messages.keySet)
//        } catch {
//          case e: Exception =>
//            e.printStackTrace()
//            logger.error("Elastic Search indexing failed: " + e)
//        }
//      }
//      // clear the already batched node ids from the list
//      nodes.subList(0, currentBatchSize).clear()
//    }
//    try //Unindexing not utilized units
//      if (unitNodes.nonEmpty) unitNodes.map(unitNodeId => esUtil.deleteDocument(unitNodeId))
//    catch {
//      case e: Exception =>
//        logger.error("Elastic Search indexing failed: " + e)
//    }
//  }


  private def getNodeForSyncing(children: List[Map[String, AnyRef]], nodes: ListBuffer[ObjectData], nodeIds: ListBuffer[String])(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): Unit = {
    if (children.nonEmpty) {
      children.foreach((child: Map[String, AnyRef]) => {
        try {
          if (StringUtils.equalsIgnoreCase("Parent", child.getOrElse("visibility", "").asInstanceOf[String])) {
            //              val childData: mutable.Map[String, AnyRef] = mutable.Map.empty[String, AnyRef]
            //              childData += child

            val nodeMetadata = mutable.Map() ++ getHierarchy(child.getOrElse("identifier", "").asInstanceOf[String], child.getOrElse("pkgVersion",1).asInstanceOf[Int], readerConfig).get // CHECK WHAT VALUE IS TO BE PUT HERE

            // TO DO - Relation related CODE is MISSING - Line 735 in Publish Finalizer

            if (nodeMetadata.getOrElse("objectType", "").asInstanceOf[String].isEmpty) {
              nodeMetadata += ("objectType" -> "Collection")
            }
            if (nodeMetadata.getOrElse("graphId", "").asInstanceOf[String].isEmpty) {
              nodeMetadata += ("graphId" -> "domain")
            }
            nodeMetadata.toMap[String, AnyRef]

            if (!nodeIds.contains(child.getOrElse("identifier", "").asInstanceOf[String])) {
              nodes += new ObjectData(child.getOrElse("identifier", "").asInstanceOf[String], nodeMetadata.toMap[String, AnyRef], Option(Map.empty[String, AnyRef]), Option(Map.empty[String, AnyRef]))
              nodeIds += child.getOrElse("identifier", "").asInstanceOf[String]
            }

            getNodeForSyncing(child.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]], nodes, nodeIds)
          }
        } catch {
          case e: Exception => logger.error("Error while generating node map. ", e)
        }
      })
    }
  }



}
