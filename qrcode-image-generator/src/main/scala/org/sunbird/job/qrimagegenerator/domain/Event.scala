package org.sunbird.job.qrimagegenerator.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest
import org.sunbird.job.qrimagegenerator.task.QRCodeImageGeneratorConfig

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  val jobName = "qrcode-image-generator"

  def eid: String = readOrDefault[String]("eid", "")

  def processId: String = readOrDefault[String]("processId", "")

  def objectId: String = readOrDefault[String]("objectId", "")

  def storageContainer: String = readOrDefault[String]("storage.container", "")

  def storagePath: String = readOrDefault[String]("storage.path", "")

  def storageFileName: String = readOrDefault[String]("storage.fileName", "")

  def imageConfigMap: Map[String, AnyRef] = readOrDefault("config", Map[String, AnyRef]())

  def dialCodes: List[Map[String, AnyRef]] = readOrDefault[List[Map[String, AnyRef]]]("dialcodes", List())

  def isValid(config: QRCodeImageGeneratorConfig): Boolean = {
    eid.nonEmpty && StringUtils.equalsIgnoreCase(config.eid, eid) && dialCodes.nonEmpty
  }

  def imageConfig(config: QRCodeImageGeneratorConfig): ImageConfig = {
    ImageConfig(
      imageConfigMap.getOrElse("errorCorrectionLevel", "").asInstanceOf[String],
      imageConfigMap.getOrElse("pixelsPerBlock", 0).asInstanceOf[Int],
      imageConfigMap.getOrElse("qrCodeMargin", 0).asInstanceOf[Int],
      imageConfigMap.getOrElse("textFontName", "").asInstanceOf[String],
      imageConfigMap.getOrElse("textFontSize", 0).asInstanceOf[Int],
      imageConfigMap.getOrElse("textCharacterSpacing", 0).asInstanceOf[Double],
      imageConfigMap.getOrElse("imageFormat", config.imageFormat).asInstanceOf[String],
      imageConfigMap.getOrElse("colourModel", "").asInstanceOf[String],
      imageConfigMap.getOrElse("imageBorderSize", 0).asInstanceOf[Int],
      imageConfigMap.getOrElse("qrCodeMarginBottom", config.imageMarginBottom).asInstanceOf[Int],
      imageConfigMap.getOrElse("imageMargin", config.imageMargin).asInstanceOf[Int]
    )
  }

}
