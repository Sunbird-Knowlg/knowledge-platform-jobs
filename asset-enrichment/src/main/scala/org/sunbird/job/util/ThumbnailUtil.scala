package org.sunbird.job.util

import java.awt.image.BufferedImage
import java.io.File

import javax.imageio.ImageIO
import org.imgscalr.Scalr
import org.slf4j.LoggerFactory

trait ThumbnailUtil {

  private[this] val logger = LoggerFactory.getLogger(classOf[ThumbnailUtil])
  private val THUMBNAIL_SIZE = 56

  def generateOutFile(inFile: File): Option[File] = {
    if (inFile != null) {
      try {
        val srcImage = ImageIO.read(inFile)
        if ((srcImage.getHeight > THUMBNAIL_SIZE) || (srcImage.getWidth > THUMBNAIL_SIZE)) {
          val scaledImage: BufferedImage = Scalr.resize(srcImage, THUMBNAIL_SIZE)
          val thumbFile = getThumbnailFileName(inFile)
          val outFile = FileUtils.createFile(thumbFile)
          ImageIO.write(scaledImage, "png", outFile)
          Some(outFile)
        } else None
      } catch {
        case ex: Exception =>
          logger.error("Please Provide Valid File Url!", ex)
          None
      }
    } else None
  }

  def getThumbnailFileName(input: File): String = {
    val outputFileName = input.getName.replaceAll("\\.", "\\.thumb\\.")
    val outputFolder = input.getParent
    s"$outputFolder/$outputFileName"
  }

}
