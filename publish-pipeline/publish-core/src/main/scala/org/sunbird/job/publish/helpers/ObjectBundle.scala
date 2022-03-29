package org.sunbird.job.publish.helpers

import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ObjectData}
import org.sunbird.job.util.{FileUtils, JSONUtil, Neo4JUtil, ScalaJsonUtil, Slug}

import java.io._
import java.net.URL
import java.text.SimpleDateFormat
import java.util
import java.util.zip.{ZipEntry, ZipOutputStream}
import java.util.{Date, Optional}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.collection.JavaConverters._

trait ObjectBundle {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectBundle])
  private val onlineMimeTypes = List("video/youtube", "video/x-youtube", "text/x-url")
  private val bundleLocation: String = "/tmp"
  private val defaultManifestVersion = "1.2"
  private val manifestFileName = "manifest.json"
  private val hierarchyFileName = "hierarchy.json"
  private val hierarchyVersion = "1.0"
  val excludeBundleMeta = List("screenshots", "posterImage", "index", "depth")

  def getBundleFileName(identifier: String, metadata: Map[String, AnyRef], pkgType: String)(implicit config: PublishConfig): String = {
    val maxAllowedContentName = config.getInt("max_allowed_content_name", 120)
    val contentName = if (metadata.getOrElse("name", "").asInstanceOf[String].length > maxAllowedContentName) metadata.getOrElse("name", "").asInstanceOf[String].substring(0, maxAllowedContentName) else metadata.getOrElse("name", "").asInstanceOf[String]
    Slug.makeSlug(contentName, isTransliterate = true) + "_" + System.currentTimeMillis() + "_" + identifier + "_" + metadata.getOrElse("pkgVersion", "") + (if (StringUtils.equals(EcarPackageType.FULL, pkgType)) ".ecar" else "_" + pkgType + ".ecar")
  }

  def getManifestData(objIdentifier: String, pkgType: String, objList: List[Map[String, AnyRef]])(implicit defCache: DefinitionCache, neo4JUtil: Neo4JUtil, defConfig: DefinitionConfig, config: PublishConfig): (List[Map[String, AnyRef]], List[Map[AnyRef, String]]) = {
    objList.map(data => {
      val identifier = data.getOrElse("identifier", "").asInstanceOf[String].replaceAll(".img", "")
      val mimeType = data.getOrElse("mimeType", "").asInstanceOf[String]
      val objectType: String = if(!data.contains("objectType") || data.getOrElse("objectType", "").asInstanceOf[String].isBlank || data.getOrElse("objectType", "").asInstanceOf[String].isEmpty) {
        val metaData = Option(neo4JUtil.getNodeProperties(identifier)).getOrElse(neo4JUtil.getNodeProperties(identifier)).asScala.toMap
        metaData.getOrElse("IL_FUNC_OBJECT_TYPE", "").asInstanceOf[String]
      } else data.getOrElse("objectType", "").asInstanceOf[String] .replaceAll("Image", "")
      val contentDisposition = data.getOrElse("contentDisposition", "").asInstanceOf[String]
      logger.info("ObjectBundle:: getManifestData:: identifier:: " + identifier + " || objectType:: " + objectType)
      val dUrlMap: Map[AnyRef, String] = getDownloadUrls(identifier, pkgType, isOnline(mimeType, contentDisposition), data)
      val updatedObj: Map[String, AnyRef] = data.map(entry =>
        if (dUrlMap.contains(entry._2)) {
          (entry._1, dUrlMap.getOrElse(entry._2.asInstanceOf[String], "").asInstanceOf[AnyRef])
        } else if (StringUtils.equalsIgnoreCase(EcarPackageType.FULL, pkgType) && StringUtils.equalsIgnoreCase(entry._1, "media")) {
          val media: List[Map[String, AnyRef]] = Optional.ofNullable(ScalaJsonUtil.deserialize[List[Map[String, AnyRef]]](entry._2.asInstanceOf[String])).orElse(List[Map[String, AnyRef]]())
          val newMedia = media.map(m => {
            m.map(entry => {
              entry._1 match {
                case "baseUrl" => (entry._1, "")
                case "src" => (entry._1, getRelativePath(identifier, entry._2.asInstanceOf[String]))
                case _ => entry
              }
            })
          })
          (entry._1, ScalaJsonUtil.serialize(newMedia))
        } else {
          if (entry._1.equalsIgnoreCase("identifier"))
            entry._1 -> entry._2.asInstanceOf[String].replaceAll(".img", "")
          else if (entry._1.equalsIgnoreCase("objectType"))
            entry._1 -> entry._2.asInstanceOf[String].replaceAll("Image", "")
          else entry
        }
      )
      val downloadUrl: String = updatedObj.getOrElse("downloadUrl", "").asInstanceOf[String]
      val dUrl: String = if (StringUtils.isNotBlank(downloadUrl)) downloadUrl else updatedObj.getOrElse("artifactUrl", "").asInstanceOf[String]
      val dMap = if (StringUtils.equalsIgnoreCase(contentDisposition, "online-only")) Map("downloadUrl" -> null) else Map("downloadUrl" -> dUrl)
      val downloadUrls: Map[AnyRef, String] = dUrlMap.keys.flatMap(key => Map(key -> identifier)).toMap

      // TODO: Addressing visibility "Parent" issue for collection children as expected by Mobile - ContentBundle.java line120 - start
      val mergedMeta = if(!identifier.equalsIgnoreCase(objIdentifier) && (objectType.equalsIgnoreCase("Content")
        || objectType.equalsIgnoreCase("Collection") || objectType.equalsIgnoreCase("QuestionSet"))) {
          updatedObj + ("visibility" -> "Parent") ++ dMap
        } else updatedObj ++ dMap
      // TODO: Addressing visibility "Parent" issue for collection children as expected by Mobile - ContentBundle.java line120 - end

      val definition: ObjectDefinition = defCache.getDefinition(objectType, defConfig.supportedVersion.getOrElse(objectType.toLowerCase, "1.0").asInstanceOf[String], defConfig.basePath)
      val enMeta = mergedMeta.filter(x => null != x._2).map(element => (element._1, convertJsonProperties(element, definition.getJsonProps())))
      (enMeta, downloadUrls)
    }).unzip
  }

  def getObjectBundle(obj: ObjectData, objList: List[Map[String, AnyRef]], pkgType: String)(implicit ec: ExecutionContext, neo4JUtil: Neo4JUtil, config: PublishConfig, defCache: DefinitionCache, defConfig: DefinitionConfig): File = {
    val bundleFileName = bundleLocation + File.separator + getBundleFileName(obj.identifier, obj.metadata, pkgType)
    val bundlePath = bundleLocation + File.separator + System.currentTimeMillis + "_temp"
    val objType = if(obj.getString("objectType", "").replaceAll("Image", "").equalsIgnoreCase("collection")) "content" else obj.getString("objectType", "").replaceAll("Image", "")
    logger.info("ObjectBundle ::: getObjectBundle ::: input objList :::: " + objList)
    // create manifest data
    val (updatedObjList, dUrls) = getManifestData(obj.identifier, pkgType, objList)
    logger.info("ObjectBundle ::: getObjectBundle ::: updatedObjList :::: " + updatedObjList)
    val downloadUrls: Map[AnyRef, List[String]] = dUrls.flatten.groupBy(_._1).map { case (k, v) => k -> v.map(_._2) }
    logger.info("ObjectBundle ::: getObjectBundle ::: downloadUrls :::: " + downloadUrls)
    val duration: String = config.getString("media_download_duration", "300 seconds")
    val downloadedMedias: List[File] = Await.result(downloadFiles(obj.identifier, downloadUrls, bundlePath), Duration.apply(duration))
    if (downloadUrls.nonEmpty && downloadedMedias.isEmpty)
      throw new InvalidInputException("Error Occurred While Downloading Bundle Media Files For : " + obj.identifier)
    val manifestFile: File = getManifestFile(obj.identifier, objType, bundlePath, updatedObjList)
    val hierarchyFile: File = getHierarchyFile(obj, bundlePath).getOrElse(new File(bundlePath))
    val fList = if (obj.hierarchy.getOrElse(Map()).nonEmpty) List(manifestFile, hierarchyFile) else List(manifestFile)
    createBundle(obj.identifier, bundleFileName, bundlePath, pkgType, downloadedMedias ::: fList)
  }

  //TODO: Enhance this method of .ecar & .zip extension
  def downloadFiles(identifier: String, files: Map[AnyRef, List[String]], bundlePath: String)(implicit ec: ExecutionContext): Future[List[File]] = {
    val futures = files.map {
      case (k, v) =>
        v.map {
          id => {
            Future {
              val destPath = s"""$bundlePath${File.separator}${StringUtils.replace(id, ".img", "")}"""
              logger.info(s"ObjectBundle ::: downloadFiles ::: Processing file: $k for : " + identifier)
              k match {
                case _: File =>
                  val file = k.asInstanceOf[File]
                  val newFile = new File(s"""${destPath}${File.separator}${file.getName}""")
                  FileUtils.copyFile(file, newFile)
                  newFile
                case _ =>
                  val url = k.asInstanceOf[String]
                  // UnknownHostException | FileNotFoundException
                  try {
                    FileUtils.downloadFile(url, destPath)
                  } catch {
                    case e: Exception => throw new InvalidInputException(s"Error while downloading file $url", e)
                  }
              }
            }
          }
        }
    }.flatten.toList
    Future.sequence(futures)
  }

  def createBundle(identifier: String, bundleFileName: String, bundlePath: String, pkgType: String, downloadedFiles: List[File]): File = {
    try {
      val stream = new FileOutputStream(bundleFileName)
      stream.write(getByteStream(identifier, downloadedFiles))
      stream.flush()
      stream.close()
      new File(bundleFileName)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw new Exception(s"Error While Generating $pkgType ECAR Bundle For : " + identifier, ex)
    } finally {
      FileUtils.deleteDirectory(new File(bundlePath))
    }
  }

  def getByteStream(identifier: String, files: List[File]): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream
    val bufferedOutputStream = new BufferedOutputStream(byteArrayOutputStream)
    val zipOutputStream = new ZipOutputStream(bufferedOutputStream)
    try {
      files.foreach(file => {
        val fileName = getFileName(file)
        try {
          zipOutputStream.putNextEntry(new ZipEntry(fileName))
          val fileInputStream = new FileInputStream(file)
          IOUtils.copy(fileInputStream, zipOutputStream)
        } catch {case ze:java.util.zip.ZipException => logger.info("ObjectBundle:: getByteStream:: ", ze.getMessage) }
        zipOutputStream.closeEntry()
      })

      if (zipOutputStream != null) {
        zipOutputStream.finish()
        zipOutputStream.flush()
        IOUtils.closeQuietly(zipOutputStream)
      }
      IOUtils.closeQuietly(bufferedOutputStream)
      IOUtils.closeQuietly(byteArrayOutputStream)
      byteArrayOutputStream.toByteArray
    } catch {
      case ex: Exception => throw new Exception("Error While Generating Byte Stream Of Bundle For : " + identifier, ex)
    }
  }

  def getFileName(file: File): String = {
    if (file.getName().toLowerCase().endsWith("manifest.json") || file.getName().endsWith("hierarchy.json")) file.getName else
      file.getParent().substring(file.getParent().lastIndexOf(File.separator) + 1) + File.separator + file.getName()
  }

  def getDownloadUrls(identifier: String, pkgType: String, isOnlineObj: Boolean, data: Map[String, AnyRef])(implicit config: PublishConfig): Map[AnyRef, String] = {
		val urlFields = pkgType match {
			case "ONLINE" => List.empty
			case "SPINE" =>
				val spineDownloadFiles: util.List[String] = if (config.getConfig().hasPath("content.downloadFiles.spine")) config.getConfig().getStringList("content.downloadFiles.spine") else util.Arrays.asList[String]("appIcon")
				spineDownloadFiles.asScala.toList
			case _ =>
				val fullDownloadFiles: util.List[String] = if (config.getConfig().hasPath("content.downloadFiles.full")) config.getConfig().getStringList("content.downloadFiles.full") else util.Arrays.asList[String]("appIcon", "grayScaleAppIcon", "artifactUrl", "itemSetPreviewUrl", "media")
				fullDownloadFiles.asScala.toList
		}
    data.filter(en => urlFields.contains(en._1) && null != en._2).flatMap(entry => {
      isOnlineObj match {
        case true => {
          if (!StringUtils.equalsIgnoreCase("artifactUrl", entry._1) && validUrl(entry._2.asInstanceOf[String])) {
            getUrlMap(identifier, pkgType, entry._1, entry._2)
          } else Map[AnyRef, String]()
        }
        case false => {
          if (StringUtils.equalsIgnoreCase(entry._1, "media")) {
            logger.info("MEDIA::  " + entry._2.asInstanceOf[String])
            val media: List[Map[String, AnyRef]] = if (null != entry._2) ScalaJsonUtil.deserialize[List[Map[String, AnyRef]]](entry._2.asInstanceOf[String]) else List()
            logger.info("MEDIA::::  " + media)
            getMediaUrl(media, identifier, pkgType)
          } else {
            entry._2 match {
              case _: File =>
                getUrlMap(identifier, pkgType, entry._1, entry._2)
              case str: String if validUrl(str) =>
                getUrlMap(identifier, pkgType, entry._1, entry._2)
              case _ => Map[AnyRef, String]()
            }
          }
        }
      }
    })
  }

  def getMediaUrl(media: List[Map[String, AnyRef]], identifier: String, pkgType: String): Map[AnyRef, String] = {
    media.map(entry => {
      val url = entry.getOrElse("baseUrl", "").asInstanceOf[String] + entry.getOrElse("src", "").asInstanceOf[String]
      if (url.isInstanceOf[String] && validUrl(url.asInstanceOf[String])) {
        Map[AnyRef, String](url -> (identifier.trim + entry.getOrElse("src", "").asInstanceOf[String]))
      } else Map[AnyRef, String]()
    }).flatten.toMap
  }

  def getUrlMap(identifier: String, pkgType: String, key: String, value: AnyRef): Map[AnyRef, String] = {
    val pkgKeys = List("artifactUrl", "downloadUrl")
    if (!pkgKeys.contains(key) || StringUtils.equalsIgnoreCase(EcarPackageType.FULL, pkgType)) {
      val fileName = value match {
        case file: File => file.getName
        case _ => value.asInstanceOf[String]
      }
      Map[AnyRef, String](value -> getRelativePath(identifier, fileName.asInstanceOf[String]))
    } else Map[AnyRef, String]()
  }

  def getRelativePath(identifier: String, value: String): String = {
    val fileName = FilenameUtils.getName(value)
    val suffix = if (fileName.endsWith(".ecar")) identifier.trim + ".zip" else Slug.makeSlug(fileName, isTransliterate = true)
    val filePath = identifier.trim + File.separator + suffix
    filePath
  }

  @throws[Exception]
  def getManifestFile(identifier: String, objType: String, bundlePath: String, objList: List[Map[String, AnyRef]]): File = {
    try {
      val file: File = new File(bundlePath + File.separator + manifestFileName)
      val header: String = s"""{"id": "sunbird.${objType.toLowerCase()}.archive", "ver": "$defaultManifestVersion" ,"ts":"$getTimeStamp", "params":{"resmsgid": "$getUUID"}, "archive":{ "count": ${objList.size}, "ttl":24, "items": """
      val mJson = header + ScalaJsonUtil.serialize(objList) + "}}"
      FileUtils.writeStringToFile(file, mJson)
      file
    } catch {
      case e: Exception => throw new Exception("Exception occurred while writing manifest file for : " + identifier, e)
    }
  }

  @throws[Exception]
  def getHierarchyFile(obj: ObjectData, bundlePath: String): Option[File] = {
    try {
      if (obj.hierarchy.getOrElse(Map()).nonEmpty) {
        val file: File = new File(bundlePath + File.separator + hierarchyFileName)
        val objType: String = obj.getString("objectType", "")
        val metadata = obj.metadata - ("IL_UNIQUE_ID", "IL_FUNC_OBJECT_TYPE", "IL_SYS_NODE_TYPE")
        val children = obj.hierarchy.get.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
        val hMap: Map[String, AnyRef] = metadata ++ Map("identifier" -> obj.identifier.replace(".img", ""), "objectType" -> objType, "children" -> children)
        val hJson = getHierarchyHeader(objType.toLowerCase()) + ScalaJsonUtil.serialize(hMap) + "}"
        FileUtils.writeStringToFile(file, hJson)
        Option(file)
      } else None
    } catch {
      case e: Exception => throw new Exception("Exception occurred while writing hierarchy file for : " + obj.identifier, e)
    }
  }

  def isOnline(mimeType: String, contentDisposition: String): Boolean = onlineMimeTypes.contains(mimeType) || StringUtils.equalsIgnoreCase(contentDisposition, "online-only")

  def validUrl(str: String): Boolean = {
    if (str.isEmpty) false else {
      try {
        new URL(str).toURI
        true
      } catch {
        case _ => false
      }
    }
  }

  def getHierarchyHeader(objType: String): String = {
    s"""{"id": "sunbird.$objType.hierarchy", "ver": "$hierarchyVersion" ,"ts":"$getTimeStamp", "params":{"resmsgid": "$getUUID"}, "$objType": """
  }

  def getTimeStamp: String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    sdf.format(new Date())
  }

  def getUUID: String = util.UUID.randomUUID().toString

  def convertJsonProperties(entry: (String, AnyRef), jsonProps: scala.List[String]): AnyRef = {
    if (jsonProps.contains(entry._1)) {
      try {
        JSONUtil.deserialize[Object](entry._2.asInstanceOf[String])
      }
      catch {
        case e: Exception => entry._2
      }
    }
    else entry._2
  }

  def getFlatStructure(children: List[Map[String, AnyRef]], childrenList: List[Map[String, AnyRef]]): List[Map[String, AnyRef]] = {
    children.flatMap(child => {
      val innerChildren = getInnerChildren(child)
      val updatedChild: Map[String, AnyRef] = if (innerChildren.nonEmpty) child ++ Map("children" -> innerChildren) else child
      val finalChild = updatedChild.filter(p => !excludeBundleMeta.contains(p._1.asInstanceOf[String]))
      val updatedChildren: List[Map[String, AnyRef]] = finalChild :: childrenList
      val result = getFlatStructure(child.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]], updatedChildren)
      finalChild :: result
    }).distinct
  }

  def getInnerChildren(child: Map[String, AnyRef]): List[Map[String, AnyRef]] = {
    val metaList: List[String] = List("identifier", "name", "objectType", "description", "index", "depth")
    child.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
      .map(ch => ch.filterKeys(key => metaList.contains(key)))
  }


}
