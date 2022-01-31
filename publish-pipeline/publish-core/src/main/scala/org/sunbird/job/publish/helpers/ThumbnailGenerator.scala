package org.sunbird.job.publish.helpers

import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import org.imgscalr.Scalr
import org.slf4j.LoggerFactory
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.util.{CloudStorageUtil, FileUtils, Slug}

import java.awt.image.BufferedImage
import java.io.File
import java.net.{HttpURLConnection, URL}
import javax.imageio.ImageIO

trait ThumbnailGenerator {

  private[this] val logger = LoggerFactory.getLogger(classOf[ThumbnailGenerator])
  private val THUMBNAIL_SIZE = 56
  private val ARTIFACT_FOLDER = "artifact"

  def generateThumbnail(obj: ObjectData)(implicit cloudStorageUtil: CloudStorageUtil): Option[ObjectData] = {
    val appIcon: String = obj.metadata.getOrElse("appIcon", "").asInstanceOf[String]
    if (StringUtils.isBlank(appIcon)) return None
    val fileName = getFileName(appIcon)
    val suffix = if (StringUtils.isNotBlank(fileName)) Slug.makeSlug(fileName, true) else Slug.makeSlug(FilenameUtils.getName(appIcon), false)
    try {
      FileUtils.copyURLToFile(obj.identifier, appIcon, suffix) match {
        case Some(file: File) => {
          logger.info("downloaded file path ::: " + file.getAbsolutePath)
          val outFile: Option[File] = generateOutFile(file)
          outFile match {
            case Some(file: File) => {
              val urlArray: Array[String] = cloudStorageUtil.uploadFile(getUploadFolderName(obj.identifier, ARTIFACT_FOLDER, obj.dbObjType.toLowerCase.replaceAll("image", "")), file, Some(true))
              Some(new ObjectData(obj.identifier, obj.metadata ++ Map("appIcon" -> urlArray(1), "posterImage" -> appIcon), obj.extData, obj.hierarchy))
            }
            case _ => {
              logger.error("Thubnail Could Not Be Generated For :" + obj.identifier)
              None
            }
          }
        }
        case _ => logger.error("ERR_INVALID_FILE_URL", "Please Provide Valid File Url!")
          None
      }
    } finally {
      org.apache.commons.io.FileUtils.deleteDirectory(new File(s"/tmp/${obj.identifier}"))
    }
  }

  def generateOutFile(inFile: File): Option[File] = {
    if (inFile != null) {
      try {
        val srcImage = ImageIO.read(inFile)
        if ((srcImage.getHeight > THUMBNAIL_SIZE) || (srcImage.getWidth > THUMBNAIL_SIZE)) {
          val scaledImage: BufferedImage = Scalr.resize(srcImage, THUMBNAIL_SIZE)
          val thumbFile = getThumbnailFileName(inFile)
          val outFile = new File(thumbFile)
          ImageIO.write(scaledImage, "png", outFile)
          Some(outFile)
        } else {
          None
        }
      } catch {
        case ex: Exception =>
          logger.error("ERR_INVALID_FILE_URL", "Please Provide Valid File Url!")
          None
      }
    } else {
      None
    }
  }

  def getThumbnailFileName(input: File): String = {
    val outputFileName = input.getName.replaceAll("\\.", "\\.thumb\\.")
    val outputFolder = input.getParent
    outputFolder + "/" + outputFileName
  }

  protected def getUploadFolderName(identifier: String, folder: String, objectType: String): String = {
    objectType.toLowerCase() + File.separator + Slug.makeSlug(identifier, true) + "/" + folder
  }

  def getFileName(fileUrl: String): String = {
    try {
      val url: URL = new URL(fileUrl)
      val httpConn: HttpURLConnection = url.openConnection.asInstanceOf[HttpURLConnection]
      if (null != httpConn && httpConn.getResponseCode == HttpURLConnection.HTTP_OK) {
        val disposition = httpConn.getHeaderField("Content-Disposition")
        if (null != httpConn)
          httpConn.disconnect()
        if (StringUtils.isNotBlank(disposition)) {
          val index = disposition.indexOf("filename=")
          if (index > 0) disposition.substring(index + 10, disposition.indexOf("\"", index + 10)) else ""
        } else ""
      } else ""
    } catch {
      case e: Exception => {
        e.printStackTrace()
        ""
      }
    }
  }

}
