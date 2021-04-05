package org.sunbird.job.util

import java.awt.image.BufferedImage
import java.io.File

import javax.imageio.ImageIO
import org.imgscalr.Scalr
import org.slf4j.LoggerFactory

trait ThumbnailUtil {

  private[this] val logger = LoggerFactory.getLogger(classOf[ThumbnailUtil])

  def generateOutFile(inFile: File, thumbnailSize: Int): Option[File] = {
    if (inFile != null) {
      try {
        val srcImage = ImageIO.read(inFile)
        if ((srcImage.getHeight > thumbnailSize) || (srcImage.getWidth > thumbnailSize)) {
          val scaledImage: BufferedImage = Scalr.resize(srcImage, thumbnailSize)
          val thumbFile = getThumbnailFileName(inFile)
          val outFile = AssetFileUtils.createFile(thumbFile)
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
