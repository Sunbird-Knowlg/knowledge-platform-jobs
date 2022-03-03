package org.sunbird.job.content.publish.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Insert, QueryBuilder, Select}
import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.helpers._
import org.sunbird.job.util._

import java.io.{File, IOException}
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import java.text.{DecimalFormat, DecimalFormatSymbols, SimpleDateFormat}
import java.util.{Date, Locale}

trait CollectionPublisher extends ObjectReader with SyncMessagesGenerator with ObjectValidator with ObjectEnrichment with EcarGenerator with ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[CollectionPublisher])
  private val level4ContentTypes = List("Course", "CourseUnit", "LessonPlan", "LessonPlanUnit")
  private val EXPANDABLE_OBJECTS = List("Collection", "QuestionSet")
  private val EXCLUDE_LEAFNODE_OBJECTS = List("Collection", "Question")
  private val INCLUDE_LEAFNODE_OBJECTS = List("QuestionSet")
  private val INCLUDE_CHILDNODE_OBJECTS = List("Collection")
  private val PUBLISHED_STATUS_LIST = List("Live", "Unlisted")
  private val COLLECTION_MIME_TYPE = "application/vnd.ekstep.content-collection"

  override def getExtData(identifier: String, pkgVersion: Double, mimeType: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[ObjectExtData] = None

  override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
    val row: Row = Option(getCollectionHierarchy(getEditableObjId(identifier, pkgVersion), readerConfig)).getOrElse(getCollectionHierarchy(identifier, readerConfig))
    if (null != row) {
      val data: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](row.getString("hierarchy"))
      Option(data)
    } else Option(Map.empty[String, AnyRef])
  }

  private def getCollectionHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Row = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(readerConfig.keyspace, readerConfig.table).
      where()
    selectWhere.and(QueryBuilder.eq("identifier", identifier))
    cassandraUtil.findOne(selectWhere.toString)
  }

  def getRelationalMetadata(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
    val row: Row = Option(getCollectionHierarchy(getEditableObjId(identifier, pkgVersion), readerConfig)).getOrElse(getCollectionHierarchy(identifier, readerConfig))
    if (null != row && row.getString("relational_metadata") != null) {
      val data: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](row.getString("relational_metadata"))
      Option(data)
    } else Option(Map.empty[String, AnyRef])
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

    val publishType = obj.getString("publish_type", "Public")
    val status = if (StringUtils.equalsIgnoreCase("Unlisted", publishType)) "Unlisted" else "Live"
    val updatedCompatibilityLevelMeta: Map[String, AnyRef] = setCompatibilityLevel(obj, updatedMeta).get + ("status" -> status)

    val isCollectionShallowCopy = isContentShallowCopy(obj)

    // Collection - Enrich Children - line 345
    val collectionHierarchy: Map[String, AnyRef] = if (isCollectionShallowCopy) {
      val originData: Map[String, AnyRef] = obj.metadata.getOrElse("originData", "").asInstanceOf[Map[String, AnyRef]]
      getHierarchy(obj.metadata("origin").asInstanceOf[String], originData.getOrElse("pkgVersion", 0).asInstanceOf[Number].doubleValue(), readerConfig).get
    } else getHierarchy(obj.identifier, obj.pkgVersion, readerConfig).get
    logger.info("CollectionPublisher:: enrichObjectMetadata:: collectionHierarchy:: " + collectionHierarchy)
    val children = if (collectionHierarchy.nonEmpty) {
      collectionHierarchy.getOrElse("children", List.empty[Map[String, AnyRef]]).asInstanceOf[List[Map[String, AnyRef]]]
    } else List.empty[Map[String, AnyRef]]
    val toEnrichChildren = children.to[ListBuffer]
    val enrichedObj: ObjectData = if (collectionHierarchy.nonEmpty && !isCollectionShallowCopy) {
      val childNodesToRemove = ListBuffer.empty[String]
      val collectionResourceChildNodes: mutable.HashSet[String] = mutable.HashSet.empty[String]
      val enrichedChildrenData = enrichChildren(toEnrichChildren, collectionResourceChildNodes, childNodesToRemove)
      val collectionChildNodes = (updatedCompatibilityLevelMeta.getOrElse("childNodes", new java.util.ArrayList()).asInstanceOf[java.util.List[String]].asScala.toList ++ collectionResourceChildNodes).distinct
      new ObjectData(obj.identifier, updatedCompatibilityLevelMeta ++ Map("childNodes" -> collectionChildNodes.filter(rec => !childNodesToRemove.contains(rec))), obj.extData, Option(collectionHierarchy + ("children" -> enrichedChildrenData.toList)))
    } else new ObjectData(obj.identifier, updatedCompatibilityLevelMeta, obj.extData, Option(collectionHierarchy ++ Map("children" -> toEnrichChildren.toList)))

    logger.info("CollectionPublisher:: enrichObjectMetadata:: Collection data after processing for : " + enrichedObj.identifier + " | Metadata : " + enrichedObj.metadata)
    logger.debug("CollectionPublisher:: enrichObjectMetadata:: Collection children data after processing : " + enrichedObj.hierarchy.get("children"))

    Some(enrichedObj)
  }

  override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = {
    val hChildren: List[Map[String, AnyRef]] = obj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
    Some(getFlatStructure(List(obj.metadata ++ obj.extData.getOrElse(Map()) ++ Map("children" -> hChildren)), List()))
  }

  override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val identifier = obj.identifier.replace(".img", "")
    val query: Insert = QueryBuilder.insertInto(readerConfig.keyspace, readerConfig.table)
    query.value(readerConfig.primaryKey(0), identifier)
    query.value("relational_metadata", null)
    val result = cassandraUtil.upsert(query.toString)
    if (result) {
      logger.info(s"relational_metadata emptied successfully for ${identifier}")
    } else {
      val msg = s"relational_metadata emptying Failed For $identifier"
      logger.error(msg)
    }
  }

  override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

  def getObjectWithEcar(obj: ObjectData, pkgTypes: List[String])(implicit ec: ExecutionContext, neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig, httpUtil: HttpUtil): ObjectData = {
   val collRelationalMetadata = getRelationalMetadata(obj.identifier, obj.pkgVersion-1, readerConfig).get
    // Line 1107 in PublishFinalizer
    val children = obj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
    val updatedChildren = updateHierarchyMetadata(children, obj.metadata, collRelationalMetadata)(config)
    val enrichedObj = processCollection(obj, updatedChildren)
    val updatedObj = updateRootChildrenList(enrichedObj, updatedChildren)
    val nodes = ListBuffer.empty[ObjectData]
    val nodeIds = ListBuffer.empty[String]
    nodes += obj
    nodeIds += obj.identifier

    val ecarMap: Map[String, String] = generateEcar(updatedObj, pkgTypes)
    val variants: java.util.Map[String, java.util.Map[String, String]] = ecarMap.map { case (key, value) => key.toLowerCase -> Map[String, String]("ecarUrl" -> value, "size" -> httpUtil.getSize(value).toString).asJava }.asJava
    logger.info("CollectionPulisher ::: getObjectWithEcar ::: variants ::: " + variants)

    val meta: Map[String, AnyRef] = Map("downloadUrl" -> ecarMap.getOrElse(EcarPackageType.SPINE, ""), "variants" -> variants, "size" -> httpUtil.getSize(ecarMap.getOrElse(EcarPackageType.SPINE, "")).asInstanceOf[AnyRef])
    new ObjectData(updatedObj.identifier, updatedObj.metadata ++ meta, updatedObj.extData, updatedObj.hierarchy)
  }

  private def setCompatibilityLevel(obj: ObjectData, updatedMeta: Map[String, AnyRef]): Option[Map[String, AnyRef]] = {
    if (level4ContentTypes.contains(obj.getString("contentType", ""))) {
      logger.info("CollectionPublisher:: setCompatibilityLevel:: setting compatibility level for content id : " + obj.identifier + " as 4.")
      Some(updatedMeta ++ Map("compatibilityLevel" -> 4.asInstanceOf[AnyRef]))
    } else Some(updatedMeta)
  }

  def getUnitsFromLiveContent(obj: ObjectData)(implicit cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): List[String] = {
    val objHierarchy = getHierarchy(obj.metadata.getOrElse("identifier", "").asInstanceOf[String], obj.metadata.getOrElse("pkgVersion", 1).asInstanceOf[Integer].doubleValue(), readerConfig).get
    val children = objHierarchy.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]]
    if (children.nonEmpty) {
      children.map(child => {
        if (child.getOrElse("visibility", "").asInstanceOf[String].equalsIgnoreCase("Parent")) {
          child.getOrElse("identifier", "").asInstanceOf[String]
        } else ""
      }).filter(rec => rec.nonEmpty)
    } else List.empty[String]
  }

  def isContentShallowCopy(obj: ObjectData): Boolean = {
    val originData: Map[String, AnyRef] = if(obj.metadata.contains("originData")) {
      obj.metadata("originData") match {
        case strValue: String => ScalaJsonUtil.deserialize[Map[String, AnyRef]](strValue)
        case mapValue:util.Map[String, AnyRef] =>  mapValue.asScala.toMap[String, AnyRef]
        case _ => obj.metadata("originData").asInstanceOf[Map[String,AnyRef]]
      }
    } else Map.empty[String, AnyRef]
    originData.nonEmpty && originData.getOrElse("copyType", "").asInstanceOf[String].equalsIgnoreCase("shallow")
  }

  def updateOriginPkgVersion(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil): ObjectData = {
    val originId = obj.metadata.getOrElse("origin", "").asInstanceOf[String]
    val originNodeMetadata = Option(neo4JUtil.getNodeProperties(originId)).getOrElse(neo4JUtil.getNodeProperties(originId))
    if (null != originNodeMetadata) {
      val originPkgVer: Double = originNodeMetadata.getOrDefault("pkgVersion", "0").toString.toDouble
      if (originPkgVer != 0) {
        val originData = obj.metadata("originData") match {
          case propVal: String => ScalaJsonUtil.deserialize[Map[String, AnyRef]](propVal) + ("pkgVersion" -> originPkgVer)
          case _ => obj.metadata.getOrElse("originData", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]] + ("pkgVersion" -> originPkgVer)
        }
        new ObjectData(obj.identifier, obj.metadata ++ Map("originData" -> originData), obj.extData, obj.hierarchy)
      } else obj
    } else obj
  }

  private def enrichChildren(toEnrichChildren: ListBuffer[Map[String, AnyRef]], collectionResourceChildNodes: mutable.HashSet[String], childNodesToRemove: ListBuffer[String])(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): ListBuffer[Map[String, AnyRef]] = {
    val newChildren = toEnrichChildren.toList
    newChildren.map(child => {
      logger.info("CollectionPublisher:: enrichChildren:: child identifier:: " + child.getOrElse("identifier", "") + " || visibility:: " + child.getOrElse("visibility", "") + " || mimeType:: " + child.getOrElse("mimeType", "") + " || objectType:: " + child.getOrElse("objectType", ""))
      if (StringUtils.equalsIgnoreCase(child.getOrElse("visibility", "").asInstanceOf[String], "Parent") && StringUtils.equalsIgnoreCase(child.getOrElse("mimeType", "").asInstanceOf[String], COLLECTION_MIME_TYPE)) {
        val updatedChildrenData = enrichChildren(child.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]].to[ListBuffer], collectionResourceChildNodes, childNodesToRemove)
        toEnrichChildren(newChildren.indexOf(child)) = child + ("children" -> updatedChildrenData.toList)
      }

      if (StringUtils.equalsIgnoreCase(child.getOrElse("visibility", "").asInstanceOf[String], "Default") && EXPANDABLE_OBJECTS.contains(child.getOrElse("objectType", "").asInstanceOf[String])) {
        val pkgVersion = child.getOrElse("pkgVersion", 0) match {
          case _: Integer => child.getOrElse("pkgVersion", 0).asInstanceOf[Integer].doubleValue()
          case _: Double => child.getOrElse("pkgVersion", 0).asInstanceOf[Double].doubleValue()
          case _ => child.getOrElse("pkgVersion", "0").toString.toDouble
        }
        val childCollectionHierarchy = getHierarchy(child.getOrElse("identifier", "").asInstanceOf[String], pkgVersion, readerConfig).get
        if (childCollectionHierarchy.nonEmpty) {
          val childNodes = childCollectionHierarchy.getOrElse("childNodes", List.empty).asInstanceOf[List[String]]
          if (childNodes.nonEmpty && INCLUDE_CHILDNODE_OBJECTS.contains(child.getOrElse("objectType", "").asInstanceOf[String])) collectionResourceChildNodes ++= childNodes.toSet[String]
          toEnrichChildren(newChildren.indexOf(child)) = childCollectionHierarchy ++ Map("index" -> child.getOrElse("index", 0).asInstanceOf[AnyRef], "depth" -> child.getOrElse("depth", 0).asInstanceOf[AnyRef], "parent" -> child.getOrElse("parent", ""), "objectType" -> child.getOrElse("objectType", "Collection").asInstanceOf[String])
        }
      }

      if (StringUtils.equalsIgnoreCase(child.getOrElse("visibility", "").asInstanceOf[String], "Default") && !EXPANDABLE_OBJECTS.contains(child.getOrElse("objectType", "").asInstanceOf[String])) {
        val childNode = Option(neo4JUtil.getNodeProperties(child.getOrElse("identifier", "").asInstanceOf[String])).getOrElse(neo4JUtil.getNodeProperties(child.getOrElse("identifier", "").asInstanceOf[String])).asScala.toMap
        if (PUBLISHED_STATUS_LIST.contains(childNode.getOrElse("status", "").asInstanceOf[String])) {
          logger.info("CollectionPublisher:: enrichChildren:: fetched child node:: " + childNode.getOrElse("IL_UNIQUE_ID", "").asInstanceOf[String] + " || objectType:: " + childNode.getOrElse("IL_FUNC_OBJECT_TYPE", "").asInstanceOf[String])
          toEnrichChildren(newChildren.indexOf(child)) = childNode ++ Map("identifier" ->childNode.getOrElse("IL_UNIQUE_ID", "").asInstanceOf[String], "objectType" ->childNode.getOrElse("IL_FUNC_OBJECT_TYPE", "").asInstanceOf[String], "index" -> child.getOrElse("index", 0).asInstanceOf[AnyRef], "parent" -> child.getOrElse("parent", "").asInstanceOf[String], "depth" -> child.getOrElse("depth", 0).asInstanceOf[AnyRef]) - ("collections", "children", "IL_FUNC_OBJECT_TYPE", "IL_SYS_NODE_TYPE","IL_UNIQUE_ID")
        } else childNodesToRemove += child.getOrElse("identifier", "").asInstanceOf[String]
      }
    })

    toEnrichChildren
  }


  private def processCollection(obj: ObjectData, children: List[Map[String, AnyRef]])(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig): ObjectData = {
    val dataMap: mutable.Map[String, AnyRef] = processChildren(children)
    logger.info("CollectionPublisher:: processCollection:: dataMap: " + dataMap)
    val updatedObj: ObjectData = if (dataMap.nonEmpty) {
      val updatedMetadataMap: Map[String, AnyRef] = dataMap.flatMap(record => {
        if (!"concepts".equalsIgnoreCase(record._1) && !"keywords".equalsIgnoreCase(record._1)) {
          Map(record._1 -> record._2.asInstanceOf[Set[String]].toArray[String])
        } else Map.empty[String, AnyRef]
      }).filter(record => record._1.nonEmpty).toMap[String, AnyRef]
      val keywords = dataMap.getOrElse("keywords", Set.empty).asInstanceOf[Set[String]].toArray[String]
      val finalKeywords: Array[String] = if (null != keywords && keywords.nonEmpty) {
        val updatedKeywords: Array[String] = if (obj.metadata.contains("keywords")) {
          obj.metadata("keywords") match {
            case _: Array[String] => keywords ++ obj.metadata.getOrElse("keywords", Array.empty).asInstanceOf[Array[String]]
            case kwValue: String => keywords ++ Array[String](kwValue)
            case _: util.Collection[String] => keywords ++ obj.metadata.getOrElse("keywords", Array.empty).asInstanceOf[util.Collection[String]].asScala.toArray[String]
            case _ => keywords
          }
        } else keywords
        updatedKeywords.filter(record => record.trim.nonEmpty).distinct
      } else if(obj.metadata.contains("keywords")) {
        obj.metadata("keywords") match {
          case _: Array[String] => obj.metadata.getOrElse("keywords", Array.empty).asInstanceOf[Array[String]]
          case kwValue: String => Array[String](kwValue)
          case _: util.Collection[String] => obj.metadata.getOrElse("keywords", Array.empty).asInstanceOf[util.Collection[String]].asScala.toArray[String]
          case _ => Array.empty[String]
        }
      } else Array.empty[String]
      new ObjectData(obj.identifier, obj.metadata ++ updatedMetadataMap + ("keywords" -> finalKeywords), obj.extData, obj.hierarchy)
    } else obj

    val enrichedObject = if(children.nonEmpty) enrichCollection(updatedObj) else updatedObj
    //    addResourceToCollection(enrichedObject, children.to[ListBuffer]) - TODO
    enrichedObject
  }

  private def processChildren(children: List[Map[String, AnyRef]]): mutable.Map[String, AnyRef] = {
    val dataMap: mutable.Map[String, AnyRef] = mutable.Map.empty
    processChildren(children, dataMap)
    dataMap
  }

  private def processChildren(children: List[Map[String, AnyRef]], dataMap: mutable.Map[String, AnyRef]): Unit = {
    if (null != children && children.nonEmpty) {
      for (child <- children) {
        mergeMap(dataMap, processChild(child))
        if (child.contains("children")) processChildren(child.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]], dataMap)
      }
    }
  }

  private def processChild(childMetadata: Map[String, AnyRef]): Map[String, AnyRef] = {
    val taggingProperties = List("language", "domain", "ageGroup", "genre", "theme", "keywords")
    val result: Map[String, AnyRef] = childMetadata.flatMap(prop => {
      if (taggingProperties.contains(prop._1)) {
        childMetadata(prop._1) match {
          case propStrValue: String => Map(prop._1 -> Set(propStrValue))
          case propListValue: List[_] => Map(prop._1 -> propListValue.toSet)
          case propVal: java.util.List[String] => Map(prop._1 -> propVal.asScala.toSet[String])
          case _ => Map.empty[String, AnyRef]
        }
      } else Map.empty[String, AnyRef]
    }).filter(rec => rec._1.nonEmpty)
    result
  }

  private def mergeMap(dataMap: mutable.Map[String, AnyRef], childDataMap: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    if (dataMap.isEmpty) dataMap ++= childDataMap
    else {
      dataMap.map(record => {
        dataMap += (record._1 -> (if (childDataMap.contains(record._1)) childDataMap(record._1).asInstanceOf[Set[String]] ++ record._2.asInstanceOf[Set[String]] else record._2.asInstanceOf[Set[String]]))
      })
      if (!dataMap.equals(childDataMap)) {
        childDataMap.map(record => {
          if (!dataMap.contains(record._1)) dataMap += record
        })
      }
    }
    dataMap
  }

  def enrichCollection(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig): ObjectData = {
    val nodeMetadata = mutable.Map.empty[String, AnyRef] ++ obj.metadata
    val contentId = obj.identifier
    logger.info("CollectionPublisher:: enrichCollection:: Processing Collection Content :" + contentId)
    val content = obj.hierarchy.get
    if (content.isEmpty) return obj
    val leafCount = getLeafNodeCount(content)
    val totalCompressedSize = getTotalCompressedSize(content, 0.0)

    val df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH))
    df.setMaximumFractionDigits(0)

    nodeMetadata.put("leafNodesCount", leafCount.asInstanceOf[AnyRef])
    nodeMetadata.put("totalCompressedSize", df.format(totalCompressedSize).toLong.asInstanceOf[AnyRef])

    nodeMetadata.put("leafNodes", updateLeafNodeIds(content))
    val mimeTypeMap: mutable.Map[String, AnyRef] = mutable.Map.empty[String, AnyRef]
    val contentTypeMap: mutable.Map[String, AnyRef] = mutable.Map.empty[String, AnyRef]
    getTypeCount(content, "mimeType", mimeTypeMap)
    getTypeCount(content, "contentType", contentTypeMap)

    val updatedContent = content ++ Map("leafNodesCount" -> leafCount, "totalCompressedSize" -> df.format(totalCompressedSize).toLong, "mimeTypesCount" -> ScalaJsonUtil.serialize(mimeTypeMap), "contentTypesCount" -> ScalaJsonUtil.serialize(contentTypeMap)).asInstanceOf[Map[String, AnyRef]]
    nodeMetadata.put("mimeTypesCount", ScalaJsonUtil.serialize(mimeTypeMap))
    nodeMetadata.put("contentTypesCount", ScalaJsonUtil.serialize(contentTypeMap))
    val uploadedFileUrl: Array[String] = generateTOC(obj, nodeMetadata.toMap)
    if(uploadedFileUrl.nonEmpty) {
      nodeMetadata.put("toc_url", uploadedFileUrl(1))
      nodeMetadata.put("s3Key", uploadedFileUrl(0))
    }

    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val updatedOn = sdf.format(new Date())
    nodeMetadata.put("sYS_INTERNAL_LAST_UPDATED_ON", updatedOn)
    val updatedMetadata: Map[String, AnyRef] = try {
      setContentAndCategoryTypes(nodeMetadata.toMap)
    } catch {
      case e: Exception => logger.error("CollectionPublisher:: enrichCollection:: Error while stringify mimeTypeCount or contentTypesCount:", e)
        nodeMetadata.toMap
    }

    new ObjectData(obj.identifier, updatedMetadata, obj.extData, Option(updatedContent))
  }

  private def updateLeafNodeIds(content: Map[String, AnyRef]): Array[String] = {
    val leafNodeIds: mutable.Set[String] = mutable.Set.empty[String]
    getLeafNodesIds(content, leafNodeIds)
    leafNodeIds.toArray
  }

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
    getLeafNodesIds(data, leafNodeIds)
    leafNodeIds.size
  }

  private def getLeafNodesIds(data: Map[String, AnyRef], leafNodeIds: mutable.Set[String]): Unit = {
    if (INCLUDE_LEAFNODE_OBJECTS.contains(data.getOrElse("objectType", "")) && StringUtils.equals(data.getOrElse("visibility", "").asInstanceOf[String], "Default")) leafNodeIds += data.getOrElse("identifier", "").asInstanceOf[String]
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
            child.getOrElse("totalCompressedSize", child.getOrElse("size", 0).asInstanceOf[Number].doubleValue).asInstanceOf[Number].doubleValue
          } else 0

        getTotalCompressedSize(child, childSize)
      }).sum
      totalCompressed + childrenSizes
    }
    else totalCompressed
  }

  def generateTOC(obj: ObjectData, content: Map[String, AnyRef])(implicit cloudStorageUtil: CloudStorageUtil, config: PublishConfig): Array[String] = {
    logger.info("CollectionPublisher:: generateTOC:: Write hierarchy to JSON File :" + obj.identifier)
    val file = new File(getTOCBasePath(obj.identifier) + "_toc.json")
    try {
      val data = ScalaJsonUtil.serialize(content)
      FileUtils.writeStringToFile(file, data, "UTF-8")
      if (file.exists) {
        logger.debug("CollectionPublisher:: generateTOC:: Upload File to cloud storage :" + file.getName)
        val uploadedFileUrl = cloudStorageUtil.uploadFile(getAWSPath(obj.identifier), file, Option.apply(true))
        if (null != uploadedFileUrl && uploadedFileUrl.length > 1) {
          logger.info("CollectionPublisher:: generateTOC:: Update cloud storage url to node" + uploadedFileUrl(1))
          uploadedFileUrl
        } else Array.empty
      } else Array.empty
    } catch {
      case e: JsonProcessingException => logger.error("CollectionPublisher:: generateTOC:: Error while parsing map object to string.", e)
        throw new InvalidInputException("CollectionPublisher:: generateTOC:: Error while parsing map object to string.", e)
      case e: Exception => logger.error("CollectionPublisher:: generateTOC:: Error while uploading file ", e)
        throw new InvalidInputException("CollectionPublisher:: generateTOC:: Error while uploading file", e)
    } finally try {
      logger.info("CollectionPublisher:: generateTOC:: Deleting Uploaded files")
      FileUtils.deleteDirectory(file.getParentFile)
    } catch {
      case e: IOException =>
        logger.error("CollectionPublisher:: generateTOC::Error while deleting file ", e)
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
    val contentType = input.getOrElse("contentType", "").asInstanceOf[String]
    val primaryCategory = input.getOrElse("primaryCategory","").asInstanceOf[String]
    val (updatedContentType, updatedPrimaryCategory): (String, String) =
      if(contentType.nonEmpty && (primaryCategory.isEmpty || primaryCategory.isBlank)) { (contentType, categoryMap.getOrDefault(contentType,"").asInstanceOf[String]) }
      else if((contentType.isEmpty || contentType.isBlank) && primaryCategory.nonEmpty) { (categoryMap.asScala.filter(entry => StringUtils.equalsIgnoreCase(entry._2.asInstanceOf[String], primaryCategory)).keys.headOption.getOrElse(""), primaryCategory) }
      else (contentType, primaryCategory)

    input ++ Map("contentType" -> updatedContentType, "primaryCategory" -> updatedPrimaryCategory)
  }

  def updateHierarchyMetadata(children: List[Map[String, AnyRef]], objMetadata: Map[String, AnyRef], collRelationalMetadata: Map[String, AnyRef])(implicit config: PublishConfig): List[Map[String, AnyRef]] = {
    if (children.nonEmpty) {
      children.map(child => {
        if (StringUtils.equalsIgnoreCase("Parent", child.getOrElse("visibility", "").asInstanceOf[String])) { //set child metadata -- compatibilityLevel, appIcon, posterImage, lastPublishedOn, pkgVersion, status
          val updatedChild = populatePublishMetadata(child, objMetadata)
          updatedChild + ("children" -> updateHierarchyMetadata(updatedChild.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]], objMetadata, collRelationalMetadata))
        } else {
          //TODO: Populate relationalMetadata here for child contents
          if (collRelationalMetadata.nonEmpty) {
            val parent = child.getOrElse("parent", "").asInstanceOf[String]
            val unitRelationalMetadata = collRelationalMetadata(parent).asInstanceOf[Map[String, AnyRef]].getOrElse("relationalMetadata", Map.empty).asInstanceOf[Map[String, AnyRef]]
            if (unitRelationalMetadata.nonEmpty) {
              val childRelationalMetadata = unitRelationalMetadata.getOrElse(child.getOrElse("identifier","").asInstanceOf[String], Map.empty).asInstanceOf[Map[String, AnyRef]]
              if(childRelationalMetadata.nonEmpty) {
                child + ("relationalMetadata" -> childRelationalMetadata)
              } else child
            } else child
          } else child
        }
      })
    } else children
  }

  private def populatePublishMetadata(content: Map[String, AnyRef], objMetadata: Map[String, AnyRef])(implicit config: PublishConfig): Map[String, AnyRef] = {
    //TODO:  For appIcon, posterImage and screenshot createThumbNail method has to be implemented.
    val leafNodeIds: mutable.Set[String] = mutable.Set.empty[String]
    getLeafNodesIds(content, leafNodeIds)

    val updatedContent = content ++
      Map("compatibilityLevel" -> (if (null != content.get("compatibilityLevel")) content.getOrElse("compatibilityLevel", 1).asInstanceOf[Number].intValue else 1),
        "lastPublishedOn" -> objMetadata("lastPublishedOn"), "pkgVersion" -> objMetadata.getOrElse("pkgVersion", 1).asInstanceOf[Number].intValue, "leafNodesCount" -> getLeafNodeCount(content),
        "leafNodes" -> leafNodeIds.toArray[String], "status" -> objMetadata("status"), "lastUpdatedOn" -> objMetadata("lastUpdatedOn"),
        "downloadUrl" -> objMetadata("downloadUrl"), "variants" -> objMetadata("variants")).asInstanceOf[Map[String, AnyRef]]

    // PRIMARY CATEGORY MAPPING IS DONE
    setContentAndCategoryTypes(updatedContent)
  }

  def publishHierarchy(children: List[Map[String, AnyRef]], obj: ObjectData, readerConfig: ExtDataConfig, config: PublishConfig)(implicit cassandraUtil: CassandraUtil): Boolean = {
    val identifier = obj.identifier.replace(".img", "")
    val contentConfig = config.asInstanceOf[ContentPublishConfig]
    val nestedFields = contentConfig.nestedFields.asScala.toList
    val nodeMetadata = obj.metadata.map(property => {
      property._2 match {
        case propVal: String => if (nestedFields.contains(property._1)) (property._1 -> ScalaJsonUtil.deserialize[AnyRef](propVal)) else property
        case _ => property
      }
    })
    val hierarchy: Map[String, AnyRef] = nodeMetadata ++ Map("children" -> children)
    val data = Map("hierarchy" -> hierarchy) ++ obj.extData.getOrElse(Map())
    val query: Insert = QueryBuilder.insertInto(readerConfig.keyspace, readerConfig.table)
    query.value(readerConfig.primaryKey.head, identifier)
    data.map(d => {
      readerConfig.propsMapping.getOrElse(d._1, "") match {
        case "blob" => query.value(d._1.toLowerCase, QueryBuilder.fcall("textAsBlob", d._2))
        case "string" => d._2 match {
          case value: String => query.value(d._1.toLowerCase, value)
          case _ => query.value(d._1.toLowerCase, ScalaJsonUtil.serialize(d._2))
        }
        case _ => query.value(d._1, d._2)
      }
    })
    logger.info(s"CollectionPublisher:: publishHierarchy:: Publishing Hierarchy data for $identifier | Query : ${query.toString}")
    val result = cassandraUtil.upsert(query.toString)
    if (result) {
      logger.info(s"CollectionPublisher:: publishHierarchy:: Hierarchy data saved successfully for $identifier")
    } else {
      val msg = s"CollectionPublisher:: publishHierarchy:: Hierarchy Data Insertion Failed For $identifier"
      logger.error(msg)
      throw new InvalidInputException(msg)
    }
    result
  }

  private def updateRootChildrenList(obj: ObjectData, nextLevelNodes: List[Map[String, AnyRef]]): ObjectData = {
    val childrenMap: List[Map[String, AnyRef]] =
      nextLevelNodes.map(record => {
        Map("identifier" -> record.getOrElse("identifier", "").asInstanceOf[String],
          "name" -> record.getOrElse("name", "").asInstanceOf[String],
          "objectType" -> record.getOrElse("objectType", "").asInstanceOf[String],
          "description" -> record.getOrElse("description", "").asInstanceOf[String],
          "index" -> record.getOrElse("index", 0).asInstanceOf[AnyRef])
      })

    new ObjectData(obj.identifier, obj.metadata ++ Map("children" -> childrenMap, "objectType" -> "content"), obj.extData, Option(obj.hierarchy.get + ("children" -> nextLevelNodes)))
  }

  def syncNodes(successObj: ObjectData, children: List[Map[String, AnyRef]], unitNodes: List[String])(implicit esUtil: ElasticSearchUtil, neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, definition: ObjectDefinition, config: PublishConfig): Map[String, Map[String, AnyRef]] = {
    val contentConfig = config.asInstanceOf[ContentPublishConfig]
    val nestedFields = contentConfig.nestedFields.asScala.toList
    val nodes = ListBuffer.empty[ObjectData]
    val nodeIds = ListBuffer.empty[String]

    getNodeForSyncing(children, nodes, nodeIds)
    logger.info("CollectionPublisher:: syncNodes:: after getNodeForSyncing:: nodes:: " + nodes + " || nodeIds:: " + nodeIds)

    val updatedUnitNodes = if (unitNodes.nonEmpty) unitNodes.filter(unitNode => !nodeIds.contains(unitNode)) else unitNodes
    logger.info("CollectionPublisher:: syncNodes:: after getNodeForSyncing:: updatedUnitNodes:: " + updatedUnitNodes)

    if (nodes.isEmpty && updatedUnitNodes.isEmpty) return Map.empty

    val errors = mutable.Map.empty[String, String]
    val messages: Map[String, Map[String, AnyRef]] = getMessages(nodes.toList, definition, nestedFields, errors)(esUtil)
    logger.info("CollectionPublisher:: syncNodes:: after getMessages:: messages:: " + messages)
    if (errors.nonEmpty) logger.error("CollectionPublisher:: syncNodes:: Error! while forming ES document data from nodes, below nodes are ignored: " + errors)
    if (messages.nonEmpty)
      try {
        logger.info("CollectionPublisher:: syncNodes:: Number of units to be synced : " + messages.size + " || " + messages.keySet)
        esUtil.bulkIndexWithIndexId(contentConfig.compositeSearchIndexName, contentConfig.compositeSearchIndexType, messages)
        logger.info("CollectionPublisher:: syncNodes:: UnitIds synced : " + messages.keySet)
      } catch {
        case e: Exception => e.printStackTrace()
          logger.error("CollectionPublisher:: syncNodes:: Elastic Search indexing failed: " + e)
      }

    try //Unindexing not utilized units
      if (updatedUnitNodes.nonEmpty) updatedUnitNodes.map(unitNodeId => esUtil.deleteDocument(unitNodeId))
    catch {
      case e: Exception =>
        logger.error("CollectionPublisher:: syncNodes:: Elastic Search indexing failed: " + e)
    }

    // Syncing collection metadata
    val doc: Map[String, AnyRef] = getDocument(new ObjectData(successObj.identifier, successObj.metadata.-("children"), successObj.extData, successObj.hierarchy), true, nestedFields)(esUtil)
    val jsonDoc: String = ScalaJsonUtil.serialize(doc)
    logger.info("CollectionPublisher:: syncNodes:: collection doc: " + jsonDoc)
    esUtil.addDocument(successObj.identifier, jsonDoc)

    messages
  }

  private def getNodeForSyncing(children: List[Map[String, AnyRef]], nodes: ListBuffer[ObjectData], nodeIds: ListBuffer[String])(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): Unit = {
    if (children.nonEmpty) {
      children.foreach((child: Map[String, AnyRef]) => {
        try {
          if (StringUtils.equalsIgnoreCase("Parent", child.getOrElse("visibility", "").asInstanceOf[String])) {
            logger.info("CollectionPublisher:: getNodeForSyncing:: child identifier: " + child.getOrElse("identifier", "").asInstanceOf[String])

            val nodeMetadata = mutable.Map() ++ child

            // TODO - Relation related CODE is MISSING - Line 735 in Publish Finalizer

            if (nodeMetadata.getOrElse("objectType", "").asInstanceOf[String].isEmpty) {
              nodeMetadata += ("objectType" -> "Collection")
            }
            if (nodeMetadata.getOrElse("graphId", "").asInstanceOf[String].isEmpty) {
              nodeMetadata += ("graph_id" -> "domain")
            }

            if (nodeMetadata.contains("children")) nodeMetadata.remove("children")

            logger.info("CollectionPublisher:: getNodeForSyncing:: nodeMetadata: " + nodeMetadata)

            if (!nodeIds.contains(child.getOrElse("identifier", "").asInstanceOf[String])) {
              nodes += new ObjectData(child.getOrElse("identifier", "").asInstanceOf[String], nodeMetadata.toMap[String, AnyRef], Option(Map.empty[String, AnyRef]), Option(Map.empty[String, AnyRef]))
              nodeIds += child.getOrElse("identifier", "").asInstanceOf[String]
            }

            getNodeForSyncing(child.getOrElse("children", List.empty).asInstanceOf[List[Map[String, AnyRef]]], nodes, nodeIds)
          }
        } catch {
          case e: Exception => logger.error("CollectionPublisher:: getNodeForSyncing:: Error while generating node map. ", e)
        }
      })
    }
  }

  def saveImageHierarchy(obj: ObjectData, readerConfig: ExtDataConfig, collRelationalMetadata: Map[String, AnyRef])(implicit cassandraUtil: CassandraUtil): Boolean = {
    val identifier = if(obj.identifier.endsWith(".img")) obj.identifier else obj.identifier+".img"
    val hierarchy: Map[String, AnyRef] = obj.hierarchy.getOrElse(Map())
    val query: Insert = QueryBuilder.insertInto(readerConfig.keyspace, readerConfig.table)
    query.value(readerConfig.primaryKey.head, identifier)
    query.value("hierarchy", ScalaJsonUtil.serialize(hierarchy))
    query.value("relational_metadata", ScalaJsonUtil.serialize(collRelationalMetadata))
    logger.info(s"CollectionPublisher:: saveImageHierarchy:: Publishing Hierarchy data for $identifier | Query : ${query.toString}")
    val result = cassandraUtil.upsert(query.toString)
    if (result) {
      logger.info(s"CollectionPublisher:: saveImageHierarchy:: Hierarchy data saved successfully for $identifier")
    } else {
      val msg = s"CollectionPublisher:: saveImageHierarchy:: Hierarchy Data Insertion Failed For $identifier"
      logger.error(msg)
      throw new InvalidInputException(msg)
    }
    result
  }

}
