package org.sunbird.job.autocreatorv2.util

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.util.{CloudStorageUtil, JSONUtil}

import java.io.{File, IOException}
import java.net.URL
import java.nio.file.{Files, Paths}
import java.util.zip.ZipFile
import scala.collection.JavaConverters._

object FileUtils {

	private[this] val logger = LoggerFactory.getLogger(classOf[FileUtils])

	def copyURLToFile(objectId: String, fileUrl: String, suffix: String): Option[File] = try {
		val fileName = getBasePath(objectId) + "/" + suffix
		val file = new File(fileName)
		org.apache.commons.io.FileUtils.copyURLToFile(new URL(fileUrl), file)
		Some(file)
	} catch {
		case e: IOException => logger.error("ERR_INVALID_FILE_URL", "Please Provide Valid File Url!")
			None
	}

	def getBasePath(objectId: String): String = {
		if (!StringUtils.isBlank(objectId))
			s"/tmp/$objectId/${System.currentTimeMillis}_temp"
		else s"/tmp/${System.currentTimeMillis}_temp"
	}

	def uploadFile(fileOption: Option[File], identifier: String, objType: String)(implicit cloudStorageUtil: CloudStorageUtil): Option[String] = {
		fileOption match {
			case Some(file: File) => {
				logger.info("FileUtils :: uploadFile :: file path :: "+file.getAbsolutePath)
				val folder = objType.toLowerCase + File.separator + identifier
				val urlArray: Array[String] = cloudStorageUtil.uploadFile(folder, file, Some(false))
				logger.info(s"FileUtils ::: uploadFile ::: url for $identifier is : ${urlArray(1)}")
				Some(urlArray(1))
			}
			case _ => None
		}
	}

	def extractPackage(file: File, basePath: String) = {
		val zipFile = new ZipFile(file)
		for (entry <- zipFile.entries().asScala) {
			val path = Paths.get(basePath + File.separator + entry.getName)
			if (entry.isDirectory) Files.createDirectories(path)
			else {
				Files.createDirectories(path.getParent)
				Files.copy(zipFile.getInputStream(entry), path)
			}
		}
	}

	def readJsonFile(filePath: String, fileName: String): Map[String, AnyRef] = {
		val source = scala.io.Source.fromFile(filePath + File.separator + fileName)
		val lines = try source.mkString finally source.close()
		JSONUtil.deserialize[Map[String, AnyRef]](lines)
	}
}

class FileUtils {}
