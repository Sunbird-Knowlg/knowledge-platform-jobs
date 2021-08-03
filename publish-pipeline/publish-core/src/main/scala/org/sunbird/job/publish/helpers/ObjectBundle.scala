package org.sunbird.job.publish.helpers

import java.io.{BufferedOutputStream, ByteArrayOutputStream, File, FileInputStream, FileOutputStream}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Optional}
import java.util.zip.{ZipEntry, ZipOutputStream}
import java.net.{HttpURLConnection, URL}

import kong.unirest.HttpResponse
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.io.{FileUtils, FilenameUtils, IOUtils}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.publish.core.{DefinitionConfig, ObjectData, Slug}
import org.sunbird.job.util.{HttpUtil, JSONUtil, ScalaJsonUtil}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

trait ObjectBundle {

	private[this] val logger = LoggerFactory.getLogger(classOf[ObjectBundle])
	private val onlineMimeTypes = List("video/youtube", "video/x-youtube", "text/x-url")
	private val bundleLocation: String = "/tmp"
	private val defaultManifestVersion = "1.2"
	private val cloudBundleFolder = "ecar_files"
	private val manifestFileName = "manifest.json"
	private val hierarchyFileName = "hierarchy.json"
	private val hierarchyVersion = "1.0"
	val excludeBundleMeta = List("screenshots", "posterImage", "index", "depth")

	def getBundleFileName(identifier: String, metadata: Map[String, AnyRef], pkgType: String) = {
		Slug.makeSlug(metadata.getOrElse("name", "").asInstanceOf[String], true) + "_" + System.currentTimeMillis() + "_" + identifier + "_" + metadata.getOrElse("pkgVersion", "") + (if (StringUtils.equals(EcarPackageType.FULL.toString, pkgType)) ".ecar" else "_" + pkgType + ".ecar")
	}

	def getManifestData(identifier: String, pkgType: String, objList: List[Map[String, AnyRef]])(implicit defCache: DefinitionCache, defConfig: DefinitionConfig): (List[Map[String, AnyRef]], List[Map[AnyRef, String]]) = {
		objList.map(data => {
			val identifier = data.getOrElse("identifier", "").asInstanceOf[String]
			val mimeType = data.getOrElse("mimeType", "").asInstanceOf[String]
			val objectType = data.getOrElse("objectType", "").asInstanceOf[String].replaceAll("Image", "")
			val contentDisposition = data.getOrElse("contentDisposition", "").asInstanceOf[String]
			val dUrlMap: Map[AnyRef, String] = getDownloadUrls(identifier, pkgType, isOnline(mimeType, contentDisposition), data)
			val updatedObj: Map[String, AnyRef] = data.map(entry =>
				if (dUrlMap.contains(entry._2)) {
					(entry._1.asInstanceOf[String], dUrlMap.getOrElse(entry._2.asInstanceOf[String], "").asInstanceOf[AnyRef])
				} else if(StringUtils.equalsIgnoreCase(EcarPackageType.FULL.toString, pkgType) && StringUtils.equalsIgnoreCase(entry._1, "media")) {
					val media: List[Map[String, AnyRef]] = Optional.ofNullable(ScalaJsonUtil.deserialize[List[Map[String, AnyRef]]](entry._2.asInstanceOf[String])).orElse(List[Map[String, AnyRef]]())
					val newMedia = media.map( m => {
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
					entry
				}
			)
			val downloadUrl: String = updatedObj.getOrElse("downloadUrl", "").asInstanceOf[String]
			val dUrl: String = if(StringUtils.isNotBlank(downloadUrl)) downloadUrl else updatedObj.getOrElse("artifactUrl", "").asInstanceOf[String]
			val dMap = if (StringUtils.equalsIgnoreCase(contentDisposition, "online-only")) Map("downloadUrl" -> null)
			else Map("downloadUrl" -> dUrl)
			val downloadUrls: Map[AnyRef, String] = dUrlMap.keys.flatMap(key => Map(key -> identifier)).toMap
			val mergedMeta = updatedObj ++ dMap
			val definition: ObjectDefinition = defCache.getDefinition(objectType, defConfig.supportedVersion.getOrElse(objectType.toLowerCase, "1.0").asInstanceOf[String], defConfig.basePath)
			val enMeta = mergedMeta.filter(x => null != x._2).map(element => (element._1, convertJsonProperties(element, definition.getJsonProps())))
			(enMeta, downloadUrls)
		}).unzip
	}

	def getObjectBundle(obj: ObjectData, objList: List[Map[String, AnyRef]], pkgType: String)(implicit ec: ExecutionContext, defCache: DefinitionCache, defConfig: DefinitionConfig): File = {
		val bundleFileName = bundleLocation + File.separator + getBundleFileName(obj.identifier, obj.metadata, pkgType)
		val bundlePath = bundleLocation + File.separator + System.currentTimeMillis + "_temp"
		val objType = obj.getString("objectType", "")
		// create manifest data
		val (updatedObjList, dUrls) = getManifestData(obj.identifier, pkgType, objList)
		logger.info("ObjectBundle ::: getObjectBundle ::: updatedObjList :::: " + updatedObjList)
		val downloadUrls: Map[AnyRef, List[String]] = dUrls.flatten.groupBy(_._1).map { case (k, v) => k -> v.map(_._2) }
		logger.info("ObjectBundle ::: getObjectBundle ::: downloadUrls :::: " + downloadUrls)
		val downloadedMedias: List[File] = Await.result(downloadFiles(obj.identifier, downloadUrls, bundlePath), Duration.apply("60 seconds"))
		if (downloadUrls.nonEmpty && downloadedMedias.isEmpty)
			throw new Exception("Error Occurred While Downloading Bundle Media Files For : " + obj.identifier)
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
							val destPath = s"""${bundlePath}${File.separator}${id}"""
							logger.info(s"ObjectBundle ::: downloadFiles ::: Processing file: $k for : " + identifier)
							k match {
								case _: File => {
									val file = k.asInstanceOf[File]
									val newFile = new File(s"""${destPath}${File.separator}${file.getName}""")
									FileUtils.copyFile(file, newFile)
									newFile
								}
								case _ => {
									val url = k.asInstanceOf[String]
									downloadFile(url, destPath)
								}
							}
						}
					}
				}
		}.flatten.toList
		Future.sequence(futures)
	}

	@throws[Exception]
	def downloadFile(fileUrl: String, basePath: String): File = {
		val url = new URL(fileUrl)
		val httpConn = url.openConnection().asInstanceOf[HttpURLConnection]
		val disposition = httpConn.getHeaderField("Content-Disposition")
		httpConn.getContentType
		httpConn.getContentLength
		val fileName = if (StringUtils.isNotBlank(disposition)) {
			val index = disposition.indexOf("filename=")
			if (index > 0)
				disposition.substring(index + 10, disposition.indexOf("\"", index + 10))
			else
				fileUrl.substring(fileUrl.lastIndexOf("/") + 1, fileUrl.length)
		} else fileUrl.substring(fileUrl.lastIndexOf("/") + 1, fileUrl.length)
		val saveFile = new File(basePath)
		if (!saveFile.exists) saveFile.mkdirs
		val saveFilePath = basePath + File.separator + fileName
		val inputStream = httpConn.getInputStream
		val outputStream = new FileOutputStream(saveFilePath)
		IOUtils.copy(inputStream, outputStream)
		val file = new File(saveFilePath)
		logger.info(System.currentTimeMillis() + " ::: Downloaded file: " + file.getAbsolutePath)
		//Slug.createSlugFile(file)
		file
	}


	def createBundle(identifier: String, bundleFileName: String, bundlePath: String, pkgType: String, downloadedFiles: List[File]) = {
		try {
			val stream = new FileOutputStream(bundleFileName)
			stream.write(getByteStream(identifier, downloadedFiles))
			stream.flush()
			stream.close()
			new File(bundleFileName)
		} catch {
			case ex: Exception => {
        ex.printStackTrace()
        throw new Exception(s"Error While Generating ${pkgType} ECAR Bundle For : " + identifier, ex)
      }
		} finally {
			FileUtils.deleteDirectory(new File(bundlePath))
		}
	}

	def getByteStream(identifier: String, files: List[File]) = {
		val byteArrayOutputStream = new ByteArrayOutputStream
		val bufferedOutputStream = new BufferedOutputStream(byteArrayOutputStream)
		val zipOutputStream = new ZipOutputStream(bufferedOutputStream)
		try {
			files.foreach(file => {
				val fileName = getFileName(file)
				zipOutputStream.putNextEntry(new ZipEntry(fileName))
				val fileInputStream = new FileInputStream(file)
				IOUtils.copy(fileInputStream, zipOutputStream)
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

	def getFileName(file: File) = {
		if (file.getName().toLowerCase().endsWith("manifest.json") || file.getName().endsWith("hierarchy.json")) file.getName else
			file.getParent().substring(file.getParent().lastIndexOf(File.separator) + 1) + File.separator + file.getName()
	}

	def getDownloadUrls(identifier: String, pkgType: String, isOnlineObj: Boolean, data: Map[String, AnyRef]): Map[AnyRef, String] = {
		val urlFields = if (StringUtils.equals("ONLINE", pkgType)) List() else List("appIcon", "grayScaleAppIcon", "artifactUrl", "itemSetPreviewUrl", "media")
		data.filter(en => urlFields.contains(en._1) && null != en._2).flatMap(entry => {
			isOnlineObj match {
				case true => {
					if (!StringUtils.equalsIgnoreCase("artifactUrl", entry._1) && validUrl(entry._2.asInstanceOf[String])) {
						getUrlMap(identifier, pkgType, entry._1, entry._2)
					} else Map[AnyRef, String]()
				}
				case false => {
					if(StringUtils.equalsIgnoreCase(entry._1, "media")){
						logger.info("MEDIA::  " + entry._2.asInstanceOf[String])
						val media: List[Map[String, AnyRef]] = if(null != entry._2) ScalaJsonUtil.deserialize[List[Map[String, AnyRef]]](entry._2.asInstanceOf[String]) else List()
						logger.info("MEDIA::::  " + media)
						getMediaUrl(media, identifier, pkgType)
					}else{
						if (entry._2.isInstanceOf[File]) {
							getUrlMap(identifier, pkgType, entry._1, entry._2)
						} else if (entry._2.isInstanceOf[String] && validUrl(entry._2.asInstanceOf[String])) {
							getUrlMap(identifier, pkgType, entry._1, entry._2)
						} else Map[AnyRef, String]()
					}
				}
			}
		})
	}

	def getMediaUrl(media: List[Map[String, AnyRef]], identifier: String, pkgType: String): Map[AnyRef, String] ={
		media.map(entry => {
			val url = entry.getOrElse("baseUrl", "").asInstanceOf[String] + entry.getOrElse("src", "").asInstanceOf[String]
			if (url.isInstanceOf[String] && validUrl(url.asInstanceOf[String])) {
				Map[AnyRef, String](url -> (identifier.trim + entry.getOrElse("src", "").asInstanceOf[String]))
			} else Map[AnyRef, String]()
		}).flatten.toMap
	}

	def getUrlMap(identifier: String, pkgType: String, key: String, value: AnyRef): Map[AnyRef, String] = {
		val pkgKeys = List("artifactUrl", "downloadUrl")
		if (!pkgKeys.contains(key) || StringUtils.equalsIgnoreCase(EcarPackageType.FULL.toString, pkgType)) {
			val fileName = if (value.isInstanceOf[File]) value.asInstanceOf[File].getName else value.asInstanceOf[String]
			Map[AnyRef, String](value -> getRelativePath(identifier, fileName.asInstanceOf[String]))
		} else Map[AnyRef, String]()
	}


	def getRelativePath(identifier: String, value: String): String = {
		val fileName = FilenameUtils.getName(value)
		val suffix = if (fileName.endsWith(".ecar")) identifier.trim + ".zip" else Slug.makeSlug(fileName, true)
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

	def isOnline(mimeType: String, contentDisposition: String) = onlineMimeTypes.contains(mimeType) || StringUtils.equalsIgnoreCase(contentDisposition, "online-only")

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

	def getTimeStamp(): String = {
		val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		sdf.format(new Date())
	}

	def getUUID(): String = util.UUID.randomUUID().toString

	def convertJsonProperties(entry: (String, AnyRef), jsonProps: scala.List[String]) = {
		if(jsonProps.contains(entry._1)) {
			try {JSONUtil.deserialize[Object](entry._2.asInstanceOf[String])}
			catch { case e: Exception => entry._2 }
		}
		else entry._2
	}

}
