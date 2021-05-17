package org.sunbird.job.assetenricment.util

import java.io.File
import org.im4java.core.{ConvertCmd, IMOperation, Info}

class ImageResizerUtil {

  def process(file: File, targetResolution: Double, width: Int, height: Int, outputFileNameSuffix: String): File = {
    val inputFileName = file.getAbsolutePath
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

  def isApplicable(fileType: String): Boolean = fileType.equalsIgnoreCase("image")

}
