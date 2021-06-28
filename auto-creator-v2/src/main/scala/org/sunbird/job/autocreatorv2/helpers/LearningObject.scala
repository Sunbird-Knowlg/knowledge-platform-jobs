package org.sunbird.job.autocreatorv2.helpers

import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.autocreatorv2.model.{ExtDataConfig, ObjectData}
import org.sunbird.job.autocreatorv2.util.{CloudStorageUtil, FileUtils}
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil, Neo4JUtil}

import java.io.File

class LearningObject(identifier: String, objectType: String, metaUrl: String, downloadUrl: String)
                    (implicit config: AutoCreatorV2Config,
                     httpUtil: HttpUtil,
                     objDef: ObjectDefinition) extends AutoCreator {

  private[this] val logger = LoggerFactory.getLogger(classOf[LearningObject])

  private val extractPath = {
    logger.info("Extracting the ECAR file.")
    extractDataZip(identifier, downloadUrl)
  }

  private val hierarchy = getHierarchy(extractPath, objectType)(config)

  private val rawData = {
    logger.info("Preparing raw data.")
    getObjectDetails(identifier, extractPath, objectType, metaUrl)
  }

  var metadata = {
    logger.info("Defining metadata.")
    rawData.filterKeys(k => !(objDef.getRelationLabels.contains(k) || objDef.externalProperties.contains(k))) ++
      Map("IL_UNIQUE_ID" -> identifier, "IL_FUNC_OBJECT_TYPE" -> objectType, "IL_SYS_NODE_TYPE" -> "DATA_NODE")
  }

  val externalData = {
    logger.info("Defining external data.")
    val extData = rawData.filterKeys(k => objDef.externalProperties.contains(k))
    if (hierarchy.nonEmpty) extData ++ Map("hierarchy" -> hierarchy) else extData
  }

  def process()(implicit cloudStorageUtil: CloudStorageUtil): Unit = {
    metadata = processCloudStore()
  }

  def processChildObj()(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, cloudStorageUtil: CloudStorageUtil, defCache: DefinitionCache) = {
    val enrObj = {
      val obj = new ObjectData(identifier, objectType, metadata, Some(externalData), Some(hierarchy))
      val chMap: Map[String, AnyRef] = getChildren(obj)(config)
      val childrenObj: Map[String, ObjectData] = processChildren(chMap)(config, neo4JUtil, cassandraUtil, cloudStorageUtil, defCache, httpUtil)
      enrichHierarchy(obj, childrenObj)(config)
    }
    metadata = enrObj.metadata
  }

  def save()(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil): Unit = {
    val obj = new ObjectData(identifier, objectType, metadata, Some(externalData), Some(hierarchy))
    val extConfig = ExtDataConfig(config.getString(obj.objectType.toLowerCase + ".keyspace", ""), objDef.getExternalTable, objDef.getExternalPrimaryKey, objDef.getExternalProps)
    saveExternalData(obj.identifier, obj.extData.getOrElse(Map()), extConfig)(cassandraUtil)
    saveGraphData(obj.identifier, obj.metadata, objDef)(neo4JUtil)
  }

  // Update methods START

  private def processCloudStore()(implicit cloudStorageUtil: CloudStorageUtil): Map[String, AnyRef] = {
    val data = config.cloudProps.filter(x => metadata.get(x).nonEmpty).flatMap(prop => {
      if (StringUtils.equalsIgnoreCase("variants", prop)) metadata.getOrElse("variants", Map()).asInstanceOf[Map[String, AnyRef]].toList
      else List((prop, metadata.getOrElse(prop, "")))
    }).toMap
    val updatedUrls: Map[String, AnyRef] = data.map(en => {
      logger.info("processCloudMeta :: key : " + en._1 + " | url : " + en._2)
      val file = FileUtils.copyURLToFile(identifier, en._2.asInstanceOf[String], FilenameUtils.getName(en._2.asInstanceOf[String]))
      val url = FileUtils.uploadFile(file, identifier, metadata.getOrElse("IL_FUNC_OBJECT_TYPE", "").asInstanceOf[String])
      (en._1, url.getOrElse(""))
    })
    logger.info("processCloudMeta :: updatedUrls : " + updatedUrls)
    metadata ++ updatedUrls.filterKeys(k => !List("spine", "online").contains(k)) ++ Map("variants" -> getVariantMap(updatedUrls))
  }

  // Update methods END


  private def getObjectDetails(identifier: String, extractPath: String, objectType: String, metaUrl: String)(implicit httpUtil: HttpUtil) : Map[String, AnyRef] = {
    val manifest = FileUtils.readJsonFile(extractPath, "manifest.json")
    val manifestMetadata: Map[String, AnyRef] = manifest.getOrElse("archive", Map()).asInstanceOf[Map[String, AnyRef]]
      .getOrElse("items", List()).asInstanceOf[List[Map[String, AnyRef]]]
      .find(p => StringUtils.equalsIgnoreCase(identifier, p.getOrElse("identifier", "").asInstanceOf[String])).getOrElse(Map())
    if (metaUrl.nonEmpty) {
      // TODO: deprecate setting "origin" after single sourcing refactoring.
      val originData = s"""{\\"identifier\\": \\"$identifier\\",\\"repository\\":\\"${metaUrl.head}\\"}"""
      val originDetails = Map[String, AnyRef]("origin" -> identifier, "originData" -> originData)
      val metadata = getMetaUrlData(metaUrl, objectType)(httpUtil) ++ originDetails
      manifestMetadata.++(metadata)
    } else manifestMetadata
  }

  private def extractDataZip(identifier: String, downloadUrl: String): String = {
    val suffix = FilenameUtils.getName(downloadUrl).replace(".ecar", ".zip")
    val zipFile: File = FileUtils.copyURLToFile(identifier, downloadUrl, suffix).get
    println("zip file path :: " + zipFile.getAbsolutePath)
    val extractPath = FileUtils.getBasePath(identifier)
    println("zip extracted path :: " + extractPath)
    FileUtils.extractPackage(zipFile, extractPath)
    extractPath
  }

  private def getMetaUrlData(metaUrl: String, objectType: String)(implicit httpUtil: HttpUtil): Map[String, AnyRef] = {
    val response = httpUtil.get(metaUrl)
    if (response.status == 200) {
      JSONUtil.deserialize[Map[String, AnyRef]](response.body).getOrElse("result", Map()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse(objectType.toLowerCase, Map()).asInstanceOf[Map[String, AnyRef]]
    } else throw new Exception("Invalid object read url for fetching metadata.")
  }

  private def getHierarchy(extractPath: String, objectType: String)(implicit config: AutoCreatorV2Config): Map[String, AnyRef] = {
    if (config.expandableObjects.contains(objectType))
      FileUtils.readJsonFile(extractPath, "hierarchy.json")
        .getOrElse(objectType.toLowerCase(), Map()).asInstanceOf[Map[String, AnyRef]]
    else Map[String, AnyRef]()
  }
}
