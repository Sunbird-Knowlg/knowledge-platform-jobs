package org.sunbird.job.content.publish.helpers

import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import org.apache.tika.Tika
import org.slf4j.LoggerFactory
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.helpers._
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, FileUtils, HttpUtil, Neo4JUtil}

import java.io.{File, IOException}
import java.nio.file.Files
import java.util
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

trait ContentPublisher extends ObjectReader with ObjectValidator with ObjectEnrichment with EcarGenerator with ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[ContentPublisher])
  private val level4MimeTypes = List("video/x-youtube", "application/pdf", "application/msword", "application/epub", "application/vnd.ekstep.h5p-archive", "text/x-url")
  private val level4ContentTypes = List("Course", "CourseUnit", "LessonPlan", "LessonPlanUnit")
  private val pragmaMimeTypes = List("video/x-youtube", "application/pdf") // Ne to check for other mimetype
  private val youtubeMimetypes = List("video/x-youtube", "video/youtube")
  private val validateArtifactUrlMimetypes = List("application/pdf", "application/epub", "application/msword")
  private val ignoreValidationMimeType = List(MimeType.Collection.toString, MimeType.Plugin_Archive.toString, MimeType.ASSETS.toString)
  private val YOUTUBE_REGEX = "^(http(s)?:\\/\\/)?((w){3}.)?youtu(be|.be)?(\\.com)?\\/.+"

  override def getExtData(identifier: String, pkgVersion: Double, mimeType: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[ObjectExtData] = {
    if (mimeType.equalsIgnoreCase("application/vnd.ekstep.ecml-archive")) {
      val ecmlBody = getContentBody(identifier, readerConfig)
      Some(ObjectExtData(Some(Map[String, AnyRef]("body" -> ecmlBody))))
    } else None
  }

  override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def enrichObjectMetadata(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig): Option[ObjectData] = {
    val contentConfig = config.asInstanceOf[ContentPublishConfig]
    val extraMeta = Map("pkgVersion" -> (obj.pkgVersion + 1).asInstanceOf[AnyRef], "lastPublishedOn" -> getTimeStamp,
      "flagReasons" -> null, "body" -> null, "publishError" -> null, "variants" -> null, "downloadUrl" -> null)
    val contentSize = obj.metadata.getOrElse("size", 0).toString.toDouble
    val configSize = contentConfig.artifactSizeForOnline
    val publishType = obj.getString("publish_type", "Public")
    val status = if (StringUtils.equalsIgnoreCase("Unlisted", publishType)) "Unlisted" else "Live"
    val updatedMeta: Map[String, AnyRef] = if (contentSize > configSize) obj.metadata ++ extraMeta ++ Map("contentDisposition" -> "online-only", "status" -> status) else obj.metadata ++ extraMeta ++ Map("status" -> status)

    val updatedCompatibilityLevel = setCompatibilityLevel(obj, updatedMeta).getOrElse(updatedMeta)
    val updatedPragma = setPragma(obj, updatedCompatibilityLevel).getOrElse(updatedCompatibilityLevel)

    //delete basePath if exists
    Files.deleteIfExists(new File(ExtractableMimeTypeHelper.getBasePath(obj.identifier, contentConfig.bundleLocation)).toPath)

    if (contentConfig.isECARExtractionEnabled && contentConfig.extractableMimeTypes.contains(obj.mimeType)) {
      ExtractableMimeTypeHelper.copyExtractedContentPackage(obj, contentConfig, "version", cloudStorageUtil)
      ExtractableMimeTypeHelper.copyExtractedContentPackage(obj, contentConfig, "latest", cloudStorageUtil)
    }
    val updatedPreviewUrl = updatePreviewUrl(obj, updatedPragma, cloudStorageUtil, contentConfig).getOrElse(updatedPragma)
    Some(new ObjectData(obj.identifier, updatedPreviewUrl, obj.extData, obj.hierarchy))
  }

  override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = {
    Some(List(obj.metadata ++ obj.extData.getOrElse(Map()).filter(p => !excludeBundleMeta.contains(p._1))))
  }

  override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

  override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

  def validateMetadata(obj: ObjectData, identifier: String, config: PublishConfig): List[String] = {
    logger.info("Validating Content metadata for : " + obj.identifier)
    val messages = ListBuffer[String]()
    val artifactUtl = obj.getString("artifactUrl", "")
    if (ignoreValidationMimeType.contains(obj.mimeType)) {
      // Validation not required. Nothing to do.
    } else if (obj.mimeType.equalsIgnoreCase(MimeType.ECML_Archive.toString)) { // Either 'body' or 'artifactUrl' is needed
      if (obj.extData.nonEmpty && ((obj.extData.get.getOrElse("body", Map.empty).isInstanceOf[Map[String, AnyRef]] && obj.extData.get.getOrElse("body", Map.empty).asInstanceOf[Map[String, AnyRef]].nonEmpty) || StringUtils.isNotBlank(artifactUtl) )) {
        //  Do nothing.
      } else messages += s"""Either 'body' or 'artifactUrl' are required for processing of ECML content for : $identifier"""
    } else {
      val allowedExtensionsWord: util.List[String] = config.asInstanceOf[ContentPublishConfig].allowedExtensionsWord

      if (StringUtils.isBlank(artifactUtl))
        messages += s"""There is no artifactUrl available for : $identifier"""
      else if (youtubeMimetypes.contains(obj.mimeType) && !isValidYouTubeUrl(artifactUtl))
        messages += s"""Invalid youtube Url = $artifactUtl for : $identifier"""
      else if (validateArtifactUrlMimetypes.contains(obj.mimeType) && !isValidUrl(artifactUtl, obj.mimeType, allowedExtensionsWord)) { // valid url check by downloading the file and then delete it
        // artifactUrl + valid url check by downloading the file
        obj.mimeType match {
          case "application/pdf" =>
            messages += s"""Error! Invalid File Extension. Uploaded file $artifactUtl is not a pdf file for : $identifier"""
          case "application/epub" =>
            messages += s"""Error! Invalid File Extension. Uploaded file $artifactUtl is not a epub file for : $identifier"""
          case "application/msword" =>
            messages += s"""Error! Invalid File Extension. | Uploaded file $artifactUtl should be among the Allowed_file_extensions for mimeType doc $allowedExtensionsWord for : $identifier"""
        }
      }
    }
    messages.toList
  }

  private def isValidYouTubeUrl(artifactUrl: String): Boolean = {
    logger.info(s"Validating if the given youtube url = $artifactUrl is valid or not.")
    Pattern.matches(YOUTUBE_REGEX, artifactUrl)
  }

  private def isValidUrl(url: String, mimeType: String, allowedExtensionsWord: util.List[String]): Boolean = {
    val destPath = s"""/tmp"""
//    val destPath = s"""$bundlePath${File.separator}${StringUtils.replace(id, ".img", "")}"""
    try {
      val file = FileUtils.downloadFile(url, destPath)
      if (exceptionChecks(mimeType, file, allowedExtensionsWord)) return true
    } catch {
      case e: Exception => e.printStackTrace()
    }
//    finally FileUtils.deleteFile(file)
    false
  }

  private def exceptionChecks(mimeType: String, file: File, allowedExtensionsWord: util.List[String]): Boolean = {
    try {
      val extension = FilenameUtils.getExtension(file.getPath)
      logger.info("Validating File For MimeType: " + file.getName)
      if (extension.nonEmpty) {
        val tika = new Tika
        val fileType = tika.detect(file)
        mimeType match {
          case "application/pdf" =>
            if (StringUtils.equalsIgnoreCase(extension, "pdf") && fileType == "application/pdf") return true
          case "application/epub" =>
            if (StringUtils.equalsIgnoreCase(extension, "epub") && fileType == "application/epub+zip") return true
          case "application/msword" =>
            if (allowedExtensionsWord.contains(extension)) return true
        }
      }
    } catch {
      case e: IOException =>
        logger.error("exceptionChecks: Error while checking mimeType.")
        e.printStackTrace()
        throw new InvalidInputException("Error while checking mimeType ", e)
    }
    false
  }

  def getObjectWithEcar(data: ObjectData, pkgTypes: List[String])(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig, httpUtil: HttpUtil): ObjectData = {
    try {
      logger.info("ContentPublisher:getObjectWithEcar: Ecar generation done for Content: " + data.identifier)

      val ecarMap: Map[String, String] = generateEcar(data, pkgTypes)
      val variants: java.util.Map[String, java.util.Map[String, String]] = ecarMap.map { case (key, value) => key.toLowerCase -> Map[String, String]("ecarUrl" -> value, "size" -> httpUtil.getSize(value).toString).asJava }.asJava
      logger.info("ContentPublisher ::: getObjectWithEcar ::: ecar map ::: " + ecarMap)
      val meta: Map[String, AnyRef] = Map("downloadUrl" -> ecarMap.getOrElse(EcarPackageType.FULL.toString, ""), "variants" -> variants)
      new ObjectData(data.identifier, data.metadata ++ meta, data.extData, data.hierarchy)
    } catch { case _: java.lang.IllegalArgumentException => throw new InvalidInputException(s"Invalid input found For $data.identifier")}
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
      val pgm: java.util.List[String] = obj.metadata.getOrElse("pragma", new java.util.ArrayList[String]()).asInstanceOf[java.util.List[String]]
      val pragma: List[String] = pgm.asScala.toList
      val value = "external"
      if (!pragma.contains(value)) {
        Some(updatedMeta ++ Map("pragma" -> (pragma ++ List(value))))
      } else None
    } else None
  }

  private def updatePreviewUrl(obj: ObjectData, updatedMeta: Map[String, AnyRef], cloudStorageUtil: CloudStorageUtil, config: ContentPublishConfig): Option[Map[String, AnyRef]] = {
    if (StringUtils.isNotBlank(obj.mimeType)) {
      logger.debug("Checking Required Fields For: " + obj.mimeType)
      obj.mimeType match {
        case "application/vnd.ekstep.content-collection" | "application/vnd.ekstep.plugin-archive"
             | "application/vnd.android.package-archive" | "assets" =>
          None
        case "application/vnd.ekstep.ecml-archive" | "application/vnd.ekstep.html-archive" | "application/vnd.ekstep.h5p-archive" =>
          val latestFolderS3Url = ExtractableMimeTypeHelper.getCloudStoreURL(obj, cloudStorageUtil, config)
          val updatedPreviewUrl = updatedMeta ++ Map("previewUrl" -> latestFolderS3Url, "streamingUrl" -> latestFolderS3Url)
          Some(updatedPreviewUrl)
        case _ =>
          val artifactUrl = obj.getString("artifactUrl", null)
          val updatedPreviewUrl = updatedMeta ++ Map("previewUrl" -> artifactUrl)
          if (config.isStreamingEnabled && !config.streamableMimeType.contains(obj.mimeType)) Some(updatedPreviewUrl ++ Map("streamingUrl" -> artifactUrl)) else Some(updatedPreviewUrl)
      }
    } else None
  }
}
