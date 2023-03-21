package org.sunbird.job.livenodepublisher.publish.helpers

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.livenodepublisher.publish.processor.{JsonParser, Media, Plugin, XmlParser}
import org.sunbird.job.livenodepublisher.task.LiveNodePublisherConfig
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.util.{CloudStorageUtil, FileUtils, Slug}
import org.xml.sax.{InputSource, SAXException}

import java.io._
import javax.xml.parsers.{DocumentBuilderFactory, ParserConfigurationException}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object ExtractableMimeTypeHelper {

  private[this] val logger = LoggerFactory.getLogger("ExtractableMimeTypeHelper")
  private val extractablePackageExtensions = List(".zip", ".h5p", ".epub")

  def getCloudStoreURL(obj: ObjectData, cloudStorageUtil: CloudStorageUtil, config: LiveNodePublisherConfig): String = {
    val path = getExtractionPath(obj, config, "latest")
    cloudStorageUtil.getURI(path, Option.apply(config.extractableMimeTypes.contains(obj.mimeType)))
  }

  private def getExtractionPath(obj: ObjectData, config: LiveNodePublisherConfig, suffix: String): String = {
    obj.mimeType match {
      case "application/vnd.ekstep.ecml-archive" => config.contentFolder + File.separator + "ecml" + File.separator + obj.identifier + "-" + suffix
      case "application/vnd.ekstep.html-archive" => config.contentFolder + File.separator + "html" + File.separator + obj.identifier + "-" + suffix
      case "application/vnd.ekstep.h5p-archive" => config.contentFolder + File.separator + "h5p" + File.separator + obj.identifier + "-" + suffix
      case _ => ""
    }
  }

  def getBasePath(objectId: String, tempFileLocation: String): String = {
    if (!StringUtils.isBlank(objectId)) tempFileLocation + File.separator + System.currentTimeMillis + "_temp" + File.separator + objectId else ""
  }

  def copyExtractedContentPackage(obj: ObjectData, contentConfig: LiveNodePublisherConfig, extractionType: String, cloudStorageUtil: CloudStorageUtil): Unit = {
    if (!isExtractedSnapshotExist(obj)) throw new InvalidInputException("Error! Snapshot Type Extraction doesn't Exists.")
    val sourcePrefix = getExtractionPath(obj, contentConfig, "snapshot")
    val destinationPrefix = getExtractionPath(obj, contentConfig, extractionType)
    cloudStorageUtil.copyObjectsByPrefix(sourcePrefix, destinationPrefix, isFolder = true)
  }

  private def isExtractedSnapshotExist(obj: ObjectData): Boolean = {
    extractablePackageExtensions.exists(key => StringUtils.endsWithIgnoreCase(obj.getString("artifactUrl", null), key))
  }

  def processECMLBody(obj: ObjectData, config: LiveNodePublisherConfig)(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil): Map[String, AnyRef] = {
    try {
      val basePath = config.bundleLocation + "/" + System.currentTimeMillis + "_tmp" + "/" + obj.identifier
      val ecmlBody = obj.extData.get.getOrElse("body", "").toString
      val ecmlType: String = getECMLType(ecmlBody)
      val ecrfObj: Plugin = getEcrfObject(ecmlType, ecmlBody)

      // localize assets - download assets to local base path (tmp folder) for validation
      localizeAssets(obj.identifier, ecrfObj, basePath, config)

      // validate assets
      val processedEcrf: Plugin = new ECMLExtractor(basePath, obj.identifier).process(ecrfObj)

      // getECMLString
      val processedEcml: String = getEcmlStringFromEcrf(processedEcrf, ecmlType)

      // write ECML String to basePath
      writeECMLFile(basePath, processedEcml, ecmlType)

      // create zip package
      val zipFileName: String = basePath + File.separator + System.currentTimeMillis + "_" + Slug.makeSlug(obj.identifier) + ".zip"
      FileUtils.createZipPackage(basePath, zipFileName)

      // upload zip file to blob and set artifactUrl
      val result: Array[String] = uploadArtifactToCloud(new File(zipFileName), obj.identifier, None, config)

      // upload local extracted directory to blob
      extractPackageInCloud(new File(zipFileName), obj, "snapshot", slugFile = true, basePath, config)

      val contentSize = (new File(zipFileName)).length

      // delete local folder
      FileUtils.deleteQuietly(basePath)

      obj.metadata ++ Map("artifactUrl" -> result(1), "cloudStorageKey" -> result(0), "size" -> contentSize.asInstanceOf[AnyRef])
    } catch {
      case ex@(_: org.sunbird.cloud.storage.exception.StorageServiceException | _: java.lang.NullPointerException | _:java.io.FileNotFoundException | _:java.io.IOException) => {
        ex.printStackTrace()
        throw new InvalidInputException(s"ECML Asset Files not found For $obj.identifier")
      }
      case anyEx: Exception => throw anyEx
    }
  }

  private def getEcrfObject(ecmlType: String, ecmlBody: String): Plugin = {
    ecmlType match {
      case "ecml" => XmlParser.parse(ecmlBody)
      case "json" => JsonParser.parse(ecmlBody)
      case _ => classOf[Plugin].newInstance()
    }
  }

  private def getEcmlStringFromEcrf(processedEcrf: Plugin, ecmlType: String): String = {
    ecmlType match {
      case "ecml" => XmlParser.toString(processedEcrf)
      case "json" => JsonParser.toString(processedEcrf)
      case _ => ""
    }
  }

  private def getECMLType(contentBody: String): String = {
    if (!StringUtils.isBlank(contentBody)) {
      if (isValidJSON(contentBody)) "json"
      else if (isValidXML(contentBody)) "ecml"
      else throw new InvalidInputException("Invalid Content Body")
    }
    else throw new InvalidInputException("Invalid Content Body. ECML content should have body.")
  }

  private def isValidJSON(contentBody: String): Boolean = {
    if (!StringUtils.isBlank(contentBody)) try {
      val objectMapper = new ObjectMapper
      objectMapper.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
      objectMapper.readTree(contentBody)
      true
    } catch {
      case ex@(_: IOException) =>
        logger.error("isValidJSON - Exception when validating the JSON :: ", ex)
        false
    }
    else false
  }

  private def isValidXML(contentBody: String): Boolean = {
    if (!StringUtils.isBlank(contentBody)) try {
      val dbFactory = DocumentBuilderFactory.newInstance
      val dBuilder = dbFactory.newDocumentBuilder
      dBuilder.parse(new InputSource(new StringReader(contentBody)))
      true
    } catch {
      case ex@(_: ParserConfigurationException | _: SAXException | _: IOException) =>
        logger.error("isValidXML - Exception when validating the XML :: ", ex)
        false
    } else false
  }

  private def writeECMLFile(basePath: String, ecml: String, ecmlType: String): Unit = {
    try {
      if (StringUtils.isBlank(ecml)) throw new InvalidInputException("[Unable to write Empty ECML File.]")
      if (StringUtils.isBlank(ecmlType)) throw new InvalidInputException("[System is in a fix between (XML & JSON) ECML Type.]")
      val file = new File(basePath + "/" + "index." + ecmlType)
      FileUtils.writeStringToFile(file, ecml)
    } catch {
      case e: Exception =>
        logger.error("writeECMLFile - Exception when write ECML file :: ", e)
        throw new Exception("[Unable to Write ECML File.]", e)
    }
  }

  private def extractPackageInCloud(uploadFile: File, obj: ObjectData, extractionType: String, slugFile: Boolean, basePath: String, config: LiveNodePublisherConfig)(implicit cloudStorageUtil: CloudStorageUtil) = {
    val file = Slug.createSlugFile(uploadFile)
    val mimeType = obj.mimeType
    validationForCloudExtraction(file, extractionType, mimeType, config)
    if (config.extractableMimeTypes.contains(mimeType)) {
      cloudStorageUtil.uploadDirectory(getExtractionPath(obj, config, extractionType), new File(basePath), Option(slugFile))
    }
  }

  private def uploadArtifactToCloud(uploadedFile: File, identifier: String, filePath: Option[String] = None, config: LiveNodePublisherConfig)(implicit cloudStorageUtil: CloudStorageUtil): Array[String] = {
    val urlArray = {
      try {
        val folder = if (filePath.isDefined) filePath.get + File.separator + config.contentFolder + File.separator + Slug.makeSlug(identifier, isTransliterate = true) + File.separator + config.artifactFolder
        else config.contentFolder + File.separator + Slug.makeSlug(identifier, isTransliterate = true) + File.separator + config.artifactFolder
        cloudStorageUtil.uploadFile(folder, uploadedFile)
      } catch {
        case e: Exception =>
          cloudStorageUtil.deleteFile(uploadedFile.getAbsolutePath, Option(false))
          logger.error("Error while uploading the Artifact file.", e)
          throw new Exception("Error while uploading the Artifact File.", e)
      }
    }
    urlArray
  }

  private def validationForCloudExtraction(file: File, extractionType: String, mimeType: String, config: LiveNodePublisherConfig): Unit = {
    if (!file.exists() || (!extractablePackageExtensions.contains("." + FilenameUtils.getExtension(file.getName)) && config.extractableMimeTypes.contains(mimeType)))
      throw new InvalidInputException("Error! File doesn't Exist.")
    if (extractionType == null)
      throw new InvalidInputException("Error! Invalid Content Extraction Type.")
  }


  private def localizeAssets(contentId: String, ecrfObj: Plugin, basePath: String, config: LiveNodePublisherConfig)(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil): Unit = {
    val medias: List[Media] = if (null != ecrfObj && null != ecrfObj.manifest) ecrfObj.manifest.medias else List.empty
    if (null != medias && medias.nonEmpty) processAssetsDownload(contentId, medias, basePath, config)
  }

  private def processAssetsDownload(contentId: String, medias: List[Media], basePath: String, config: LiveNodePublisherConfig)(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil): Map[String, String] = {
    val downloadResultMap = Await.result(downloadAssetFiles(contentId, medias, basePath, config), Duration.apply(config.assetDownloadDuration))
    downloadResultMap.filter(record => record.nonEmpty).flatten.toMap
  }

  private def downloadAssetFiles(identifier: String, mediaFiles: List[Media], basePath: String, config: LiveNodePublisherConfig)(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil): Future[List[Map[String, String]]] = {
    val futures = mediaFiles.map(mediaFile => {
      logger.info(s"ExtractableMimeTypeHelper ::: downloadAssetFiles ::: Processing file: ${mediaFile.id} for : " + identifier)
      if (!StringUtils.equals("youtube", mediaFile.`type`) && !StringUtils.isBlank(mediaFile.src) && !StringUtils.isBlank(mediaFile.`type`)) {
        val downloadPath = if (isWidgetTypeAsset(mediaFile.`type`)) basePath + "/" + "widgets" else basePath + "/" + "assets"
        val subFolder = {
          if (!mediaFile.src.startsWith("http") && !mediaFile.src.startsWith("https")) {
            val f = new File(mediaFile.src)
            if (f.exists) f.delete
            StringUtils.stripStart(f.getParent, "/")
          } else ""
        }
        val fDownloadPath = if (StringUtils.isNotBlank(subFolder)) downloadPath + File.separator + subFolder + File.separator else downloadPath + File.separator
        createDirectoryIfNeeded(fDownloadPath)
        logger.info(s"ExtractableMimeTypeHelper ::: downloadAssetFiles ::: fDownloadPath: $fDownloadPath & src : ${mediaFile.src}")

        if (mediaFile.src.startsWith("https://") || mediaFile.src.startsWith("http://")) {
          FileUtils.downloadFile(mediaFile.src, fDownloadPath)
        }
        else {
          if (mediaFile.src.contains("assets/public")) {
            try {
              cloudStorageUtil.downloadFile(fDownloadPath, StringUtils.replace(mediaFile.src, "//", "/").substring(mediaFile.src.indexOf("assets/public") + 14))
            } catch {
              case _: Exception => cloudStorageUtil.downloadFile(fDownloadPath, mediaFile.src.substring(mediaFile.src.indexOf("assets/public") + 13))
            }
          }
          else if (mediaFile.src.startsWith(File.separator)) {
            cloudStorageUtil.downloadFile(fDownloadPath, StringUtils.replace(mediaFile.src.substring(1), "//", "/"))
          } else {
            cloudStorageUtil.downloadFile(fDownloadPath, StringUtils.replace(mediaFile.src, "//", "/"))
          }
        }
        val downloadedFile = new File(fDownloadPath + mediaFile.src.split("/").last)
        logger.info("Downloaded file : " + mediaFile.src + " - " + downloadedFile + " | [Content Id '" + identifier + "']")

        Map(mediaFile.id -> downloadedFile.getName)
      } else Map.empty[String, String]
    })
    Future(futures)
  }

  private def isWidgetTypeAsset(assetType: String): Boolean = StringUtils.equalsIgnoreCase(assetType, "js") || StringUtils.equalsIgnoreCase(assetType, "css") || StringUtils.equalsIgnoreCase(assetType, "json") || StringUtils.equalsIgnoreCase(assetType, "plugin")

  private def createDirectoryIfNeeded(directoryName: String): Unit = {
    val theDir = new File(directoryName)
    if (!theDir.exists) theDir.mkdirs
  }

}
