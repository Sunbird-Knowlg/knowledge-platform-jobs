package org.sunbird.job.assetenricment.util

import org.slf4j.LoggerFactory

import java.io.File
import javax.activation.MimetypesFileTypeMap

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

}
