package org.sunbird.job.content.publish.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers._
import org.sunbird.job.publish.util.CloudStorageUtil
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._

trait ContentPublisher extends ObjectReader with ObjectValidator with ObjectEnrichment with EcarGenerator with ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[ContentPublisher])
  private val level4MimeTypes = List("video/x-youtube", "application/pdf", "application/msword", "application/epub", "application/vnd.ekstep.h5p-archive", "text/x-url")
  private val level4ContentTypes = List("Course", "CourseUnit", "LessonPlan", "LessonPlanUnit")
  private val pragmaMimeTypes = List("video/x-youtube", "application/pdf")

  override def getExtData(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def enrichObjectMetadata(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, config: PublishConfig): Option[ObjectData] = {
    val updatedMeta: Map[String, AnyRef] = obj.metadata ++ Map("pkgVersion" -> (obj.pkgVersion + 1).asInstanceOf[AnyRef],
      "lastPublishedOn" -> getTimeStamp, "flagReasons" -> null, "body" -> null, "publishError" -> null,
      "variants" -> null, "downloadUrl" -> null)
    val updatedCompatibilityLevel = setCompatibilityLevel(obj, updatedMeta).getOrElse(updatedMeta)
    val updatedPragma = setPragma(obj, updatedCompatibilityLevel).getOrElse(updatedCompatibilityLevel)
    val updatedPreviewUrl = updatePreviewUrl(obj, updatedPragma, config.asInstanceOf[ContentPublishConfig]).getOrElse(updatedPragma)
    Some(new ObjectData(obj.identifier, updatedPreviewUrl, obj.extData, obj.hierarchy))
  }

  override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = {
    Some(List(obj.metadata ++ obj.extData.getOrElse(Map()).filter(p => !excludeBundleMeta.contains(p._1.asInstanceOf[String]))))
  }

  override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

  override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

  def validateMetadata(obj: ObjectData, identifier: String): List[String] = {
    logger.info("Validating Content metadata for : " + obj.identifier)
    val messages = ListBuffer[String]()

    if (StringUtils.isBlank(obj.metadata.getOrElse("artifactUrl", "").asInstanceOf[String])) messages += s"""There is no artifactUrl available for : $identifier"""
    messages.toList
  }

  def getObjectWithEcar(data: ObjectData, pkgTypes: List[String])(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil, defCache: DefinitionCache, defConfig: DefinitionConfig): ObjectData = {
    logger.info("ContentPublisher:getObjectWithEcar: Ecar generation done for Content: " + data.identifier)
    val ecarMap: Map[String, String] = generateEcar(data, pkgTypes)
    val variants: java.util.Map[String, String] = ecarMap.map { case (key, value) => key.toLowerCase -> value }.asJava
    logger.info("ContentPublisher ::: getObjectWithEcar ::: ecar map ::: " + ecarMap)
    val meta: Map[String, AnyRef] = Map("downloadUrl" -> ecarMap.getOrElse("FULL", ""), "variants" -> variants)
    new ObjectData(data.identifier, data.metadata ++ meta, data.extData, data.hierarchy)
  }

  private def setCompatibilityLevel(obj: ObjectData, updatedMeta: Map[String, AnyRef]): Option[Map[String, AnyRef]] = {
    if (level4MimeTypes.contains(obj.mimeType)
      || level4ContentTypes.contains(obj.getString("contentType", ""))) {
      logger.info("setting compatibility level for content id : " + obj.identifier + " as 4.")
      Some(updatedMeta ++ Map("compatibilityLevel" -> 4.asInstanceOf[AnyRef]))
    } else None
  }

  private def setPragma(obj: ObjectData, updatedMeta: Map[String, AnyRef]): Option[Map[String, AnyRef]] = {
    if (pragmaMimeTypes.contains(obj.mimeType)) {
      val pragma:List[String] = obj.metadata.getOrElse("pragma", List()).asInstanceOf[List[String]]
      val value = "external"
      if(!pragma.contains(value)) {
        Some(updatedMeta ++ Map("pragma" -> (pragma ++ List(value))))
      } else None
    } else None
  }

  private def updatePreviewUrl(obj: ObjectData, updatedMeta: Map[String, AnyRef], config: ContentPublishConfig): Option[Map[String, AnyRef]] = {
    if (StringUtils.isNotBlank(obj.mimeType)) {
      logger.debug("Checking Required Fields For: " + obj.mimeType)
      obj.mimeType match {
        case "application/vnd.ekstep.content-collection" | "application/vnd.ekstep.plugin-archive"
             | "application/vnd.android.package-archive" | "assets" =>
        None
        case "application/vnd.ekstep.ecml-archive" | "application/vnd.ekstep.html-archive" | "application/vnd.ekstep.h5p-archive" =>
//          copyLatestS3Url(obj)
          None
        case _ =>
          val artifactUrl = obj.getString("artifactUrl", null)
          val updatedPreviewUrl = updatedMeta ++ Map("previewUrl" -> artifactUrl)
          if (config.streamableMimeType.contains(obj.mimeType)) Some(updatedPreviewUrl ++ Map("streamingUrl"-> artifactUrl)) else Some(updatedPreviewUrl)
      }
    } else None
  }
}
