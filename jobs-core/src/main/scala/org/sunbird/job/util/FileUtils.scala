package org.sunbird.job.util

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.io.{File, FileInputStream, FileOutputStream, IOException}
import java.net.{HttpURLConnection, URL}
import java.nio.file.{Files, Path, Paths}
import java.util.zip.{ZipEntry, ZipFile, ZipOutputStream}
import scala.collection.JavaConverters._

object FileUtils {

  private[this] val logger = LoggerFactory.getLogger(classOf[FileUtils])

  def copyURLToFile(objectId: String, fileUrl: String, suffix: String): Option[File] = try {
    val fileName = getBasePath(objectId) + "/" + suffix
    val file = new File(fileName)
    org.apache.commons.io.FileUtils.copyURLToFile(new URL(fileUrl), file)
    Some(file)
  } catch {
    case e: IOException => logger.error(s"Please Provide Valid File Url. Url: $fileUrl and objectId: $objectId!", e)
      None
  }

  def getBasePath(objectId: String): String = {
    if (!StringUtils.isBlank(objectId))
      s"/tmp/$objectId/${System.currentTimeMillis}_temp"
    else s"/tmp/${System.currentTimeMillis}_temp"
  }

  def uploadFile(fileOption: Option[File], identifier: String, objType: String)(implicit cloudStorageUtil: CloudStorageUtil): Option[String] = {
    fileOption match {
      case Some(file: File) =>
        logger.info("FileUtils :: uploadFile :: file path :: " + file.getAbsolutePath)
        val folder = objType.toLowerCase + File.separator + identifier
        val urlArray: Array[String] = cloudStorageUtil.uploadFile(folder, file, Some(false))
        logger.info(s"FileUtils ::: uploadFile ::: url for $identifier is : ${urlArray(1)}")
        Some(urlArray(1))
      case _ => None
    }
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
    logger.info("FileUtils :: downloadFile :: " + System.currentTimeMillis() + " ::: Downloaded file: " + file.getAbsolutePath)
    file
  }

  def extractPackage(file: File, basePath: String): Unit = {
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

  def createFile(fileName: String): File = {
    val file = new File(fileName)
    org.apache.commons.io.FileUtils.touch(file)
    file
  }

  def writeStringToFile(file: File, data: String, encoding: String = "UTF-8"): Unit = {
    org.apache.commons.io.FileUtils.writeStringToFile(file, data, encoding)
  }

  def copyFile(file: File, newFile: File): Unit = {
    org.apache.commons.io.FileUtils.copyFile(file, newFile)
  }

  def readJsonFile(filePath: String, fileName: String): Map[String, AnyRef] = {
    val source = scala.io.Source.fromFile(filePath + File.separator + fileName)
    val lines = try source.mkString finally source.close()
    JSONUtil.deserialize[Map[String, AnyRef]](lines)
  }

  def deleteDirectory(file: File): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(file)
  }

  def deleteQuietly(fileName: String): Unit = {
    org.apache.commons.io.FileUtils.deleteQuietly(org.apache.commons.io.FileUtils.getFile(fileName).getParentFile)
  }

  def createZipPackage(basePath: String, zipFileName: String): Unit =
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

  def zipIt(zipFile: String, fileList: List[String], sourceFolder: String): Unit = {
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
      case e: IOException =>
        logger.error("Error! Something Went Wrong While Creating the ZIP File: " + e.getMessage, e)
        throw new Exception("Something Went Wrong While Creating the ZIP File", e)
    } finally if (zos != null) zos.close()
  }
}

class FileUtils {}
