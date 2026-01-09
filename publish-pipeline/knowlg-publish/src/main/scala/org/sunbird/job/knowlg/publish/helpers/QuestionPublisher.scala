package org.sunbird.job.knowlg.publish.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder, Select}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core._
import org.sunbird.job.publish.helpers._
import org.sunbird.job.util._

import java.io.File
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

trait QuestionPublisher extends ObjectReader with ObjectValidator with ObjectEnrichment with EcarGenerator with ObjectUpdater {
  private val bundleLocation: String = "/tmp"
  private val indexFileName = "index.json"
  private val defaultManifestVersion = "1.2"
  private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPublisher])

  override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, config: PublishConfig): Option[Map[String, AnyRef]] = None

  override def enrichObjectMetadata(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig): Option[ObjectData] = {
    val pkgVersion = obj.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number].intValue() + 1
    val publishType = obj.getString("publish_type", "Public")
    val status = if (StringUtils.equals("Private", publishType)) "Unlisted" else "Live"
    val prevStatus = obj.metadata.getOrElse("status", "").asInstanceOf[String]
    val updatedMeta = obj.metadata ++ Map("pkgVersion" -> pkgVersion.asInstanceOf[AnyRef], "status" -> status, "prevStatus" -> prevStatus, "publishError" -> null, "variants" -> null, "downloadUrl" -> null)
    Some(new ObjectData(obj.identifier, updatedMeta, obj.extData, obj.hierarchy))
  }

  def validateQuestion(obj: ObjectData, identifier: String): List[String] = {
    logger.info("Validating Question External Data For : " + obj.identifier)
    val messages = ListBuffer[String]()
    if (obj.extData.getOrElse(Map()).getOrElse("body", "").asInstanceOf[String].isEmpty) messages += s"""There is no body available for : $identifier"""
    val interactionTypes = obj.metadata.getOrElse("interactionTypes", new util.ArrayList[String]()).asInstanceOf[util.List[String]].asScala.toList
    if (interactionTypes.nonEmpty) {
      if (obj.extData.get.getOrElse("responseDeclaration", "").asInstanceOf[String].isEmpty) messages += s"""There is no responseDeclaration available for : $identifier"""
      if (obj.extData.get.getOrElse("interactions", "").asInstanceOf[String].isEmpty) messages += s"""There is no interactions available for : $identifier"""
    } else {
      if (obj.extData.getOrElse(Map()).getOrElse("answer", "").asInstanceOf[String].isEmpty) messages += s"""There is no answer available for : $identifier"""
    }
    if(!StringUtils.equalsIgnoreCase("1.1", obj.metadata.getOrElse("schemaVersion", "1.0").asInstanceOf[String])
      && obj.extData.getOrElse(Map()).getOrElse("editorState", "").asInstanceOf[String].isEmpty)
      messages += s"""There is no editorState available for : $identifier"""

    messages.toList
  }

  override def getExtData(identifier: String, pkgVersion: Double, mimeType: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, config: PublishConfig): Option[ObjectExtData] = {
    val row: Row = Option(getQuestionData(getEditableObjId(identifier, pkgVersion), readerConfig)).getOrElse(getQuestionData(identifier, readerConfig))
    val data = if (null != row) Option(readerConfig.propsMapping.keySet.map(prop => prop -> row.getString(prop.toLowerCase())).toMap.filter(p => StringUtils.isNotBlank(p._2))) else Option(Map[String, AnyRef]())
    Option(ObjectExtData(data))
  }

  def getQuestionData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Row = {
    logger.info("QuestionPublisher ::: getQuestionData ::: Reading Question External Data For : " + identifier)
    val select = QueryBuilder.select()
    val extProps: Set[String] = readerConfig.propsMapping.keySet
    if (null != extProps && !extProps.isEmpty) {
      extProps.foreach(prop => {
        if ("blob".equalsIgnoreCase(readerConfig.propsMapping.getOrElse(prop, "").asInstanceOf[String]))
          select.fcall("blobAsText", QueryBuilder.column(prop)).as(prop)
        else
          select.column(prop).as(prop)
      })
    }
    val selectWhere: Select.Where = select.from(readerConfig.keyspace, readerConfig.table).where().and(QueryBuilder.eq(readerConfig.primaryKey.head, identifier))
    logger.info("Cassandra Fetch Query :: " + selectWhere.toString)
    cassandraUtil.findOne(selectWhere.toString)
  }

  override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
    None
  }

  override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
    None
  }

  override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val extData = obj.extData.getOrElse(Map())
    val identifier = obj.identifier.replace(".img", "")
    val columns = readerConfig.propsMapping.keySet
    val query = QueryBuilder.update(readerConfig.keyspace, readerConfig.table)
    val clause: Clause = QueryBuilder.eq(readerConfig.primaryKey.head, identifier)
    query.where.and(clause)
    columns.foreach(col => {
      readerConfig.propsMapping.getOrElse(col, "").asInstanceOf[String].toLowerCase match {
        case "blob" => extData.getOrElse(col, "") match {
          case value: String => query.`with`(QueryBuilder.set(col, QueryBuilder.fcall("textAsBlob", extData.getOrElse(col, ""))))
          case _ => query.`with`(QueryBuilder.set(col, QueryBuilder.fcall("textAsBlob", JSONUtil.serialize(extData.getOrElse(col, "")))))
        }
        case "string" => extData.getOrElse(col, "") match {
          case value: String => query.`with`(QueryBuilder.set(col, extData.getOrElse(col, null)))
          case _ => query.`with`(QueryBuilder.set(col, JSONUtil.serialize(extData.getOrElse(col, ""))))
        }
        case _ => query.`with`(QueryBuilder.set(col, extData.getOrElse(col, null)))
      }
    })

    logger.info(s"Updating Question in Cassandra For $identifier : ${query.toString}")
    val result = cassandraUtil.upsert(query.toString)
    if (result) {
      logger.info(s"Question Updated Successfully For $identifier")
    } else {
      val msg = s"Question Update Failed For $identifier"
      logger.error(msg)
      throw new Exception(msg)
    }
  }

  override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val query = QueryBuilder.delete().from(readerConfig.keyspace, readerConfig.table)
      .where(QueryBuilder.eq("identifier", obj.dbId))
      .ifExists
    cassandraUtil.session.execute(query.toString)
  }

  override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = {
    Some(List(obj.metadata ++ obj.extData.getOrElse(Map()).filter(p => !excludeBundleMeta.contains(p._1))))
  }

  def getObjectWithEcar(data: ObjectData, pkgTypes: List[String])(implicit ec: ExecutionContext, neo4JUtil: Neo4JUtil, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig, httpUtil: HttpUtil): ObjectData = {
    logger.info("QuestionPublisher:generateECAR: Ecar generation done for Question: " + data.identifier)
    val ecarMap: Map[String, String] = generateEcar(data, pkgTypes)
    val variants: java.util.Map[String, java.util.Map[String, String]] = ecarMap.map { case (key, value) => key.toLowerCase -> Map[String, String]("ecarUrl" -> value, "size" -> httpUtil.getSize(value).toString).asJava }.asJava
    logger.info("QuestionPublisher ::: generateECAR ::: ecar map ::: " + ecarMap)
    val meta: Map[String, AnyRef] = Map("downloadUrl" -> ecarMap.getOrElse(EcarPackageType.FULL.toString, ""), "variants" -> variants)
    new ObjectData(data.identifier, data.metadata ++ meta, data.extData, data.hierarchy)
  }

  def updateArtifactUrl(obj: ObjectData, pkgType: String)(implicit ec: ExecutionContext, neo4JUtil: Neo4JUtil, cloudStorageUtil: CloudStorageUtil, defCache: DefinitionCache, defConfig: DefinitionConfig, config: PublishConfig, httpUtil: HttpUtil): ObjectData = {
    val bundlePath = bundleLocation + File.separator + obj.identifier + File.separator + System.currentTimeMillis + "_temp"
    try {
      val objType = obj.getString("objectType", "")
      val objList = getDataForEcar(obj).getOrElse(List())
      val (updatedObjList, dUrls) = getManifestData(obj.identifier, objType, pkgType, objList)
      val downloadUrls: Map[AnyRef, List[String]] = dUrls.asInstanceOf[List[Map[AnyRef, String]]].flatten.groupBy(_._1).map { case (k, v) => k -> v.map(_._2) }
      logger.info("QuestionPublisher ::: updateArtifactUrl ::: downloadUrls :::: " + downloadUrls)
      val duration: String = config.getString("media_download_duration", "300 seconds")
      val downloadedMedias: List[File] = Await.result(downloadFiles(obj.identifier, downloadUrls, bundlePath), Duration.apply(duration))
      if (downloadUrls.nonEmpty && downloadedMedias.isEmpty)
        throw new Exception("Error Occurred While Downloading Bundle Media Files For : " + obj.identifier)

      getIndexFile(obj.identifier, objType, bundlePath, updatedObjList)

      // create zip package
      val zipFileName: String = bundlePath + File.separator + obj.identifier + "_" + System.currentTimeMillis + ".zip"
      FileUtils.createZipPackage(bundlePath, zipFileName)

      // upload zip file to blob and set artifactUrl
      val result: Array[String] = uploadArtifactToCloud(new File(zipFileName), obj.identifier)

      val updatedMeta = obj.metadata ++ Map("artifactUrl" -> result(1))
      new ObjectData(obj.identifier, updatedMeta, obj.extData, obj.hierarchy)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw new Exception(s"Error While Generating $pkgType ECAR Bundle For : " + obj.identifier, ex)
    } finally {
      FileUtils.deleteDirectory(new File(bundlePath))
    }
  }

  @throws[Exception]
  def getIndexFile(identifier: String, objType: String, bundlePath: String, objList: List[Map[String, AnyRef]]): File = {
    try {
      val file: File = new File(bundlePath + File.separator + indexFileName)
      val header: String = s"""{"id": "sunbird.${objType.toLowerCase()}.archive", "ver": "$defaultManifestVersion" ,"ts":"$getTimeStamp", "params":{"resmsgid": "$getUUID"}, "archive":{ "count": ${objList.size}, "ttl":24, "items": """
      val mJson = header + ScalaJsonUtil.serialize(objList) + "}}"
      FileUtils.writeStringToFile(file, mJson)
      file
    } catch {
      case e: Exception => throw new Exception("Exception occurred while writing manifest file for : " + identifier, e)
    }
  }

  private def uploadArtifactToCloud(uploadFile: File, identifier: String)(implicit cloudStorageUtil: CloudStorageUtil): Array[String] = {
    var urlArray = new Array[String](2)
    // Check the cloud folder convention to store artifact.zip file with Mahesh
    try {
      val folder = "question" + File.separator + Slug.makeSlug(identifier, isTransliterate = true)
      urlArray = cloudStorageUtil.uploadFile(folder, uploadFile)
    } catch {
      case e: Exception =>
        cloudStorageUtil.deleteFile(uploadFile.getAbsolutePath, Option(false))
        logger.error("Error while uploading the Artifact file.", e)
        throw new Exception("Error while uploading the Artifact File.", e)
    }
    urlArray
  }

}