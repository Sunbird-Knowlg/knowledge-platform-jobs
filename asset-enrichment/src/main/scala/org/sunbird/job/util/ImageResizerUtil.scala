package org.sunbird.job.util

import java.io.File

import org.apache.commons.io.FilenameUtils
import org.im4java.core.{ConvertCmd, IMOperation, Info}

class ImageResizerUtil {

  def process(file: File, targetResolution: Double, width: Int, height: Int, outputFileNameSuffix: String): File = {
    val inputFileName = file.getAbsolutePath
//    val outputFileName = FilenameUtils.getName(file.getAbsolutePath.replaceAll("\\.", s"\\.${outputFileNameSuffix}\\."))
    val outputFileName = file.getAbsolutePath.replaceAll("\\.", s"\\.${outputFileNameSuffix}\\.")
    // set optimize width and height
    val ow = width
    val oh = height
    // create command
    val cmd = new ConvertCmd
    // create the operation, add images and operators/options
    val op = new IMOperation
    op.addImage(inputFileName)
    op.resize(ow, oh)
    if (targetResolution.toInt > 0) op.resample(targetResolution.toInt)
    op.addImage(outputFileName)
    // execute the operation
    cmd.run(op)
    // replace the file
    if (outputFileNameSuffix.equalsIgnoreCase("out")) replace(file, new File(outputFileName)) else new File(outputFileName)
  }

  def replace(input: File, output: File): File = {
    val inputFile = input.getAbsolutePath
    input.delete
    output.renameTo(new File(inputFile))
    output
  }

  def process(file: File): File = {
    try {
      val inputFileName = file.getAbsolutePath
      val imageInfo = new Info(inputFileName, false)
      // Find the image size and resolution
      val width = imageInfo.getImageWidth
      val height = imageInfo.getImageHeight
      val resString = imageInfo.getProperty("Resolution")
      val xresd = if (resString != null) {
        val res = resString.split("x")
        val xres = if (res.nonEmpty) res(0)
        else "150"
        xres.toDouble
      } else 150.toDouble
      // Resize 50%
      val ow = width / 2
      val oh = height / 2
      // Target resolution - reduce to half
      val targetResolution = xresd / 2
      return process(file, targetResolution, ow, oh, "out")
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
    null
  }

  def isApplicable(fileType: String): Boolean = fileType.equalsIgnoreCase("image")

}
