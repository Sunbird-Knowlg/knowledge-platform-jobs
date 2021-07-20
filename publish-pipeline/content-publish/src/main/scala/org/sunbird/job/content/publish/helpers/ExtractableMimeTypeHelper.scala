package org.sunbird.job.content.publish.helpers

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.content.publish.processor.{JsonParser, Plugin, XmlParser}
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.publish.core.{ExtDataConfig, ObjectData, Slug}
import org.sunbird.job.publish.util.CloudStorageUtil
import org.sunbird.job.util.CassandraUtil
import org.xml.sax.{InputSource, SAXException}

import java.io._
import java.nio.file.{Files, Path, Paths}
import java.util.zip.{ZipEntry, ZipOutputStream}
import javax.xml.parsers.{DocumentBuilderFactory, ParserConfigurationException}
import scala.concurrent.ExecutionContext

object ExtractableMimeTypeHelper {

  private[this] val logger = LoggerFactory.getLogger("ExtractableMimeTypeHelper")
  val extractableMimeTypes = List("application/vnd.ekstep.ecml-archive", "application/vnd.ekstep.html-archive", "application/vnd.ekstep.plugin-archive", "application/vnd.ekstep.h5p-archive")
  private val extractablePackageExtensions = List(".zip", ".h5p", ".epub")

  def getS3URL(obj: ObjectData, cloudStorageUtil: CloudStorageUtil, config: ContentPublishConfig): String = {
    val path = getExtractionPath(obj, config, "latest")
    cloudStorageUtil.getURI(path, Option.apply(extractableMimeTypes.contains(obj.mimeType)))
  }

  private def getExtractionPath(obj: ObjectData, config: ContentPublishConfig, prefix: String):String = {
    obj.mimeType match {
      case "application/vnd.ekstep.ecml-archive" => config.contentFolder + File.separator + "ecml" + File.separator + obj.identifier + "-" + prefix
      case "application/vnd.ekstep.html-archive" => config.contentFolder + File.separator + "html" + File.separator + obj.identifier + "-" + prefix
      case "application/vnd.ekstep.h5p-archive" => config.contentFolder + File.separator + "h5p" + File.separator + obj.identifier + "-" + prefix
      case _ => ""
    }
  }

  def getBasePath(objectId: String, tempFileLocation: String): String = {
    if (!StringUtils.isBlank(objectId)) tempFileLocation + File.separator + System.currentTimeMillis + "_temp" + File.separator + objectId else ""
  }

  def copyExtractedContentPackage(obj: ObjectData, contentConfig: ContentPublishConfig, extractionType: String, cloudStorageUtil: CloudStorageUtil): Unit = {
    if (!isExtractedSnapshotExist(obj)) throw new Exception("Error! Snapshot Type Extraction doesn't Exists.")
    val sourcePrefix = getExtractionPath(obj, contentConfig, "snapshot")
    val destinationPrefix = getExtractionPath(obj, contentConfig, extractionType)
    cloudStorageUtil.copyObjectsByPrefix(sourcePrefix, destinationPrefix)
  }

  private def isExtractedSnapshotExist(obj: ObjectData): Boolean = {
    val artifactUrl = obj.getString("artifactUrl", null)
    isValidSnapshotFile(artifactUrl)
  }

  private def isValidSnapshotFile(artifactUrl: String): Boolean = {
    val isValidSnapshot = extractablePackageExtensions.filter(key => StringUtils.endsWithIgnoreCase(artifactUrl, key))
    isValidSnapshot.nonEmpty
  }

  def getContentBody(identifier: String, readerConfig: ExtDataConfig) (implicit cassandraUtil: CassandraUtil): String = {
    // fetch content body from cassandra
    val select = QueryBuilder.select()
    select.fcall("blobAsText", QueryBuilder.column("body")).as("body")
    val selectWhere: Select.Where = select.from(readerConfig.keyspace, readerConfig.table).where().and(QueryBuilder.eq("content_id", identifier))
    logger.info("Cassandra Fetch Query :: "+ selectWhere.toString)
    val row = cassandraUtil.findOne(selectWhere.toString)
    if(null != row) row.getString("body") else ""
  }

  def processECMLBody(obj: ObjectData, config: ContentPublishConfig)(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil): Map[String, AnyRef] = {
    val basePath = config.bundleLocation + "/" + System.currentTimeMillis + "_tmp" + "/" + obj.identifier
    val contentId = obj.metadata.getOrElse("identifier","").asInstanceOf[String]
    val ecmlBody = obj.extData.get.getOrElse("body", "").toString
    val ecmlType: String = getECMLType(ecmlBody)
    val ecrfObj: Plugin = getEcrfObject(ecmlType, ecmlBody)

    // validate assets
    val processedEcrf: Plugin = new ECMLExtractor(basePath, contentId).process(ecrfObj)

    // getECMLString
    val processedEcml: String = getEcmlStringFromEcrf(processedEcrf, ecmlType)

    // write ECML String to basePath
    writeECMLFile(basePath, processedEcml, ecmlType)

    // create zip package
    val zipFileName: String = basePath + File.separator + System.currentTimeMillis + "_" + Slug.makeSlug(contentId) + ".zip"
    createZipPackage(basePath,zipFileName)

    // upload zip file to blob and set artifactUrl
    val result: Array[String] = uploadArtifactToCloud(new File(zipFileName), contentId, None, config)

    // upload local extracted directory to blob
    extractPackageInCloud(new File(zipFileName), obj, "snapshot", slugFile = true, basePath, config)

    obj.metadata ++ Map("artifactUrl" -> result(1), "cloudStorageKey" -> result(0), "s3Key" -> result(0))
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
      else throw new Exception("Invalid Content Body")
    }
    else throw new Exception("Invalid Content Body")
  }

  private def isValidJSON(contentBody: String): Boolean = {
    if (!StringUtils.isBlank(contentBody)) try {
      val objectMapper = new ObjectMapper
      objectMapper.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
      objectMapper.readTree(contentBody)
      true
    } catch {
      case _: IOException =>  false
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
      case _: ParserConfigurationException | _: SAXException | _: IOException => false
    } else false
  }

  private def writeECMLFile(basePath: String, ecml: String, ecmlType: String): Unit = {
    try {
      if (StringUtils.isBlank(ecml)) throw new Exception( "[Unable to write Empty ECML File.]")
      if (StringUtils.isBlank(ecmlType)) throw new Exception("[System is in a fix between (XML & JSON) ECML Type.]")
      val file = new File(basePath + "/" + "index." + ecmlType)
      FileUtils.writeStringToFile(file, ecml, "UTF-8")
    } catch {
      case e: Exception =>
        throw new Exception("[Unable to Write ECML File.]", e)
    }
  }

  private def createZipPackage(basePath: String, zipFileName: String): Unit =
    if (!StringUtils.isBlank(zipFileName)) {
      logger.info("Creating Zip File: " + zipFileName)
      val fileList: List[String] = generateFileList(basePath)
      zipIt(zipFileName, fileList, basePath)
    }

  private def generateFileList(sourceFolder: String): List[String] =
    Files.walk(Paths.get(new File(sourceFolder).getPath)).toArray()
      .map(path => path.asInstanceOf[Path])
      .filter(path => Files.isRegularFile(path))
      .map(path => generateZipEntry(path.toString, sourceFolder)).toList


  private def generateZipEntry(file: String, sourceFolder: String): String = file.substring(sourceFolder.length, file.length)

  private def zipIt(zipFile: String, fileList: List[String], sourceFolder: String): Unit = {
    val buffer = new Array[Byte](1024)
    var zos: ZipOutputStream = null
    try {
      zos = new ZipOutputStream(new FileOutputStream(zipFile))
      logger.info("Creating Zip File: " + zipFile)
      fileList.foreach(file => {
        val ze = new ZipEntry(file)
        zos.putNextEntry(ze)
        val in = new FileInputStream(sourceFolder + File.separator + file)
        try {
          var len = in.read(buffer)
          while (len > 0) {
            zos.write(buffer, 0, len)
            len = in.read(buffer)
          }
        } finally if (in != null) in.close()
        zos.closeEntry()
      })
    } catch {
      case e: IOException => logger.error("Error! Something Went Wrong While Creating the ZIP File: " + e.getMessage, e)
    } finally if (zos != null) zos.close()
  }

  private def extractPackageInCloud(uploadFile: File, obj: ObjectData, extractionType: String, slugFile: Boolean, basePath: String, config: ContentPublishConfig)(implicit cloudStorageUtil: CloudStorageUtil) = {
    val file = Slug.createSlugFile(uploadFile)
    val mimeType = obj.mimeType
    validationForCloudExtraction(file, extractionType, mimeType)
    if(extractableMimeTypes.contains(mimeType)){
      try {
        cloudStorageUtil.uploadDirectory(getExtractionPath(obj, config, extractionType), new File(basePath), Option(slugFile))
      } catch {
        case _:Exception => println("Remove try catch")
      }
    }
  }

  private def uploadArtifactToCloud(uploadedFile: File, identifier: String, filePath: Option[String] = None, config: ContentPublishConfig)(implicit cloudStorageUtil: CloudStorageUtil): Array[String] = {
    val urlArray = {
      try {
        val folder = if (filePath.isDefined) filePath.get + File.separator + config.contentFolder + File.separator + Slug.makeSlug(identifier, isTransliterate = true) + File.separator + config.artifactFolder else config.contentFolder + File.separator + Slug.makeSlug(identifier, isTransliterate = true) + File.separator + config.artifactFolder
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

  private def validationForCloudExtraction(file: File, extractionType: String, mimeType: String): Unit = {
    if(!file.exists() || (!extractablePackageExtensions.contains("." + FilenameUtils.getExtension(file.getName)) && extractableMimeTypes.contains(mimeType)))
      throw new Exception("Error! File doesn't Exist.")
    if(null == extractionType)
      throw new Exception("Error! Invalid Content Extraction Type.")
  }

}