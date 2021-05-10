package org.sunbird.job.util

import java.io.{File, IOException}
import java.net.URL

import javax.activation.MimetypesFileTypeMap
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

object AssetFileUtils {

  private[this] val logger = LoggerFactory.getLogger("org.sunbird.job.util.AssetFileUtils")
  val mimeTypesMap: MimetypesFileTypeMap = initializeMimeTypes()

  def initializeMimeTypes(): MimetypesFileTypeMap = {
    val mimeTypes = new MimetypesFileTypeMap
    mimeTypes.addMimeTypes("image png jpg jpeg")
    mimeTypes.addMimeTypes("audio mp3 ogg wav")
    mimeTypes.addMimeTypes("video mp4")
    mimeTypes
  }

  def getFileType(file: File): String = {
    if (file.isDirectory) "Directory" else {
      val mimeType = mimeTypesMap.getContentType(file)
      mimeType.split("/")(0) match {
        case "image" => "Image"
        case "audio" => "Audio"
        case "video" => "Video"
        case _ => "Other"
      }
    }
  }

  def deleteDirectory(file: File): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(file)
  }

  def copyURLToFile(objectId: String, fileUrl: String, suffix: String): Option[File] = try {
    val fileName = getBasePath(objectId) + "/" + suffix
    val file = new File(fileName)
    org.apache.commons.io.FileUtils.copyURLToFile(new URL(fileUrl), file)
    Some(file)
  } catch {
    case e: IOException => logger.error(s"Please Provide Valid File Url. Url: ${fileUrl} and objectId: ${objectId}!", e)
      None
  }

  def getBasePath(objectId: String): String = {
    if (!StringUtils.isBlank(objectId))
      s"/tmp/$objectId/${System.currentTimeMillis}_temp"
    else ""
  }

  def createFile(fileName: String): File = {
    val file = new File(fileName)
    org.apache.commons.io.FileUtils.touch(file)
    file
  }

}
