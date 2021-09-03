package org.sunbird.job.content.publish.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.content.util.HierarchyConstants
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.helpers._
import org.sunbird.job.publish.util.CloudStorageUtil
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil, ScalaJsonUtil}

import java.util.concurrent.CompletionException
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

trait CollectionPublisher extends ObjectReader with ObjectValidator with ObjectEnrichment with EcarGenerator with ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[CollectionPublisher])
  private val level4ContentTypes = List("Course", "CourseUnit", "LessonPlan", "LessonPlanUnit")

  override def getExtData(identifier: String, pkgVersion: Double, mimeType: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[ObjectExtData] = None

  override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
    val row: Row = Option(getCollectionHierarchy(getEditableObjId(identifier, pkgVersion), readerConfig)).getOrElse(getCollectionHierarchy(identifier, readerConfig))
    if (null != row) {
      val data: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](row.getString("hierarchy"))
      Option(data)
    } else Option(Map())
  }

  def getCollectionHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
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

    val updatedCompatibilityLevel = setCompatibilityLevel(obj, updatedMeta).getOrElse(updatedMeta)

    val isCollectionShallowCopy =  isContentShallowCopy(obj)

    // Collection - Enrich Children - line 345
    val collectionHierarchy: Map[String, AnyRef] = if (isCollectionShallowCopy) {
      val originData: Map[String, AnyRef] = obj.metadata.getOrElse("originData","").asInstanceOf[Map[String,AnyRef]]
      getHierarchy(obj.metadata.get("origin").asInstanceOf[String], originData.getOrElse("pkgVersion", 0.0).asInstanceOf[Double], readerConfig).get
    } else getHierarchy(obj.identifier, obj.pkgVersion, readerConfig).get
    logger.debug("Hierarchy for content : " + obj.identifier + " : " + collectionHierarchy)

//    if (collectionHierarchy.nonEmpty) {
//     val children = collectionHierarchy.get("children").asInstanceOf[List[Map[String, AnyRef]]]
//      if (!isCollectionShallowCopy) {
//        val collectionResourceChildNodes: Set[String] = new HashSet[String]
//        enrichChildren(children, collectionResourceChildNodes, node)
//        if (!(collectionResourceChildNodes.isEmpty)) {
//          val collectionChildNodes: List[String] = getList(node.getMetadata.get(ContentWorkflowPipelineParams.childNodes.name))
//          collectionChildNodes.addAll(collectionResourceChildNodes)
//          node.getMetadata.put(ContentWorkflowPipelineParams.childNodes.name, collectionChildNodes)
//        }
//      }
//    }
//
//    logger.info("Collection processing started for content: " + node.getIdentifier)
//    processCollection(node, children)
//    logger.info("Collection processing done for content: " + node.getIdentifier)
//    logger.info("Collection data after processing for : " + node.getIdentifier + " | Metadata : " + node.getMetadata)
//    logger.info("Collection children data after processing : " + children)



    Some(new ObjectData(obj.identifier, updatedCompatibilityLevel, obj.extData, obj.hierarchy))
  }

  override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = {
    Some(List(obj.metadata ++ obj.extData.getOrElse(Map()).filter(p => !excludeBundleMeta.contains(p._1))))
  }

  override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

  override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

  def validateMetadata(obj: ObjectData, identifier: String): List[String] = {
    logger.info("Validating Collection metadata for : " + obj.identifier)
    val messages = ListBuffer[String]()
   if (StringUtils.isBlank(obj.getString("artifactUrl", "")))
      messages += s"""There is no artifactUrl available for : $identifier"""

    messages.toList
  }

  def getObjectWithEcar(data: ObjectData, pkgTypes: List[String])(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig, httpUtil: HttpUtil): ObjectData = {
    logger.info("CollectionPulisher:getObjectWithEcar: Ecar generation done for Content: " + data.identifier)
    val ecarMap: Map[String, String] = generateEcar(data, pkgTypes)
    val variants: java.util.Map[String, java.util.Map[String, String]] = ecarMap.map { case (key, value) => key.toLowerCase -> Map[String, String]("ecarUrl" -> value, "size" -> httpUtil.getSize(value).toString).asJava }.asJava
    logger.info("CollectionPulisher ::: getObjectWithEcar ::: ecar map ::: " + ecarMap)
    val meta: Map[String, AnyRef] = Map("downloadUrl" -> ecarMap.getOrElse(EcarPackageType.FULL.toString, ""), "variants" -> variants)
    new ObjectData(data.identifier, data.metadata ++ meta, data.extData, data.hierarchy)
  }

  private def setCompatibilityLevel(obj: ObjectData, updatedMeta: Map[String, AnyRef]): Option[Map[String, AnyRef]] = {
    if (level4ContentTypes.contains(obj.getString("contentType", ""))) {
      logger.info("setting compatibility level for content id : " + obj.identifier + " as 4.")
      Some(updatedMeta ++ Map("compatibilityLevel" -> 4.asInstanceOf[AnyRef]))
    } else None
  }

  def getUnitsFromLiveContent(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil): List[String] = {
    val metaData = Option(neo4JUtil.getNodeProperties(obj.identifier)).getOrElse(neo4JUtil.getNodeProperties(obj.identifier)).asScala.toMap
    metaData.getOrElse("childNodes",List.empty).asInstanceOf[List[String]]
  }

  def isContentShallowCopy(obj: ObjectData): Boolean = {
    val originData: Map[String, AnyRef] = obj.metadata.getOrElse("originData","").asInstanceOf[Map[String,AnyRef]]
    if (originData != null && !originData.isEmpty && StringUtils.isNoneBlank(originData.get("copyType").asInstanceOf[String]) && StringUtils.equalsIgnoreCase(originData.get("copyType").asInstanceOf[String], "shallow")) true
    else false
  }

  def updateOriginPkgVersion(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil): ObjectData = {
    val originId = obj.metadata.getOrElse("origin", "").asInstanceOf[String]
    val originNodeMetadata = Option(neo4JUtil.getNodeProperties(originId)).getOrElse(neo4JUtil.getNodeProperties(originId))
    if (null != originNodeMetadata) {
      val originPkgVer = originNodeMetadata.getOrDefault("pkgVersion", 0.0).asInstanceOf[Double]
      if (originPkgVer ne 0.0) {
        val originData = obj.metadata.getOrElse("originData","").asInstanceOf[Map[String, AnyRef]] ++ Map("pkgVersion" -> originPkgVer)
        new ObjectData(obj.identifier, obj.metadata ++ Map("originData" -> originData) , obj.extData, obj.hierarchy)
      } else obj
    } else obj
  }


}
