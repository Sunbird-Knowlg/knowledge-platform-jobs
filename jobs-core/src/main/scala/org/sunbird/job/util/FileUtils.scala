package org.sunbird.job.util

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.io.{File, FileInputStream, FileOutputStream, IOException}
import java.net.{HttpURLConnection, URL}
import java.util.zip.{ZipEntry, ZipOutputStream}

object FileUtils {

  private val logger = LoggerFactory.getLogger("org.sunbird.job.util.FileUtils")

  @throws[Exception]
  def downloadFile(fileUrl: String, basePath: String): File = {
    logger.info("FileUtils :: downloadFile :: URL: " + fileUrl + " | basePath: " + basePath)
    try {
      val url = new URL(fileUrl)
      val conn = url.openConnection()
      val (inputStream, fileName) = conn match {
        case httpConn: HttpURLConnection =>
          val disposition = httpConn.getHeaderField("Content-Disposition")
          val name = if (StringUtils.isNotBlank(disposition)) {
            val index = disposition.indexOf("filename=")
            if (index > 0)
              disposition.substring(index + 10, disposition.indexOf("\"", index + 10))
            else
              fileUrl.substring(fileUrl.lastIndexOf("/") + 1, fileUrl.length)
          } else fileUrl.substring(fileUrl.lastIndexOf("/") + 1, fileUrl.length)
          (httpConn.getInputStream, name)
        case _ =>
          val name = fileUrl.substring(fileUrl.lastIndexOf("/") + 1)
          (conn.getInputStream, name)
      }
      
      val saveFile = new File(basePath)
      if (!saveFile.exists) saveFile.mkdirs
      val saveFilePath = basePath + File.separator + fileName
      val outputStream = new FileOutputStream(saveFilePath)
      try {
        IOUtils.copy(inputStream, outputStream)
      } finally {
        IOUtils.closeQuietly(inputStream)
        IOUtils.closeQuietly(outputStream)
      }
      val file = new File(saveFilePath)
      logger.info("FileUtils :: downloadFile :: " + System.currentTimeMillis() + " ::: Downloaded file: " + file.getAbsolutePath)
      file
    } catch {
      case e: Exception =>
        logger.error("FileUtils :: downloadFile :: Error for URL " + fileUrl + " : " + e.getMessage, e)
        throw e
    }
  }

  def deleteFile(file: File): Unit = {
    if (file.exists) {
      if (file.isDirectory) {
        val files = file.listFiles
        if (files != null && files.nonEmpty) {
          for (f <- files) {
            deleteFile(f)
          }
        }
      }
      file.delete
    }
  }

  def deleteDirectory(file: File): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(file)
  }

  def deleteQuietly(file: File): Unit = {
    org.apache.commons.io.FileUtils.deleteQuietly(file)
  }

  def deleteQuietly(path: String): Unit = {
    org.apache.commons.io.FileUtils.deleteQuietly(new File(path))
  }

  def copyFile(srcFile: File, destFile: File): Unit = {
    org.apache.commons.io.FileUtils.copyFile(srcFile, destFile)
  }

  def copyURLToFile(source: URL, destination: File): Unit = {
    org.apache.commons.io.FileUtils.copyURLToFile(source, destination)
  }

  def copyURLToFile(identifier: String, fileUrl: String, suffix: String): Option[File] = {
    try {
      val url = new URL(fileUrl)
      val path = s"/tmp/${identifier}"
      val file = new File(path, suffix)
      org.apache.commons.io.FileUtils.copyURLToFile(url, file)
      Some(file)
    } catch {
      case e: Exception =>
        logger.error(s"FileUtils :: copyURLToFile :: Error for URL $fileUrl : ${e.getMessage}", e)
        None
    }
  }

  def getBasePath(identifier: String): String = {
    if (StringUtils.isNotBlank(identifier))
      s"/tmp${File.separator}${identifier}${File.separator}${System.currentTimeMillis}"
    else ""
  }

  def createFile(filePath: String): File = {
    val file = new File(filePath)
    if (!file.exists()) {
      file.getParentFile.mkdirs()
      file.createNewFile()
    }
    file
  }

  def createZipPackage(sourceDirectory: String, zipFileName: String): Unit = {
    zip(sourceDirectory, zipFileName)
  }

  def writeStringToFile(file: File, data: String, encoding: String = "UTF-8"): Unit = {
    org.apache.commons.io.FileUtils.writeStringToFile(file, data, encoding)
  }

  def zipIt(zipFileName: String, fileList: List[String], tempFilePath: String): Unit = {
    val zipOut = new ZipOutputStream(new FileOutputStream(zipFileName))
    fileList.foreach(file => {
      val fileToZip = new File(tempFilePath + File.separator + file)
      val fis = new FileInputStream(fileToZip)
      val zipEntry = new ZipEntry(fileToZip.getName)
      zipOut.putNextEntry(zipEntry)
      IOUtils.copy(fis, zipOut)
      zipOut.closeEntry()
      fis.close()
    })
    zipOut.close()
  }

  def zip(sourceDirectory: String, zipFileName: String): Unit = {
    var zos: ZipOutputStream = null
    try {
      zos = new ZipOutputStream(new FileOutputStream(zipFileName))
      val folder = new File(sourceDirectory)
      zipFile(folder, folder, zos)
    } catch {
      case e: IOException =>
        logger.error("Error! Something Went Wrong While Creating the ZIP File: " + e.getMessage, e)
        throw new Exception("Something Went Wrong While Creating the ZIP File", e)
    } finally if (zos != null) zos.close()
  }

  private def zipFile(fileToZip: File, baseFolder: File, zos: ZipOutputStream): Unit = {
    if (fileToZip.isDirectory) {
      val files = fileToZip.listFiles
      if (files != null) {
        for (file <- files) {
          zipFile(file, baseFolder, zos)
        }
      }
    } else {
      val fis = new FileInputStream(fileToZip)
      val entryPath = if (fileToZip.getPath.length > baseFolder.getPath.length) {
        fileToZip.getPath.substring(baseFolder.getPath.length + 1)
      } else fileToZip.getName
      val zipEntry = new ZipEntry(entryPath)
      zos.putNextEntry(zipEntry)
      IOUtils.copy(fis, zos)
      zos.closeEntry()
      fis.close()
    }
  }
}

class FileUtils {}
