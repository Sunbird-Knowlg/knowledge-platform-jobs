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

  def imageConfig: Map[String, AnyRef] = readOrDefault("config", Map[String, AnyRef]())

  def dialCodes: List[Map[String, AnyRef]] = readOrDefault[List[Map[String, AnyRef]]]("dialcodes", List())

  def imageFormat: String = readOrDefault[String]("config.imageFormat", "png")

  def isValid(config: QRCodeImageGeneratorConfig): Boolean = {
    eid.nonEmpty && StringUtils.equalsIgnoreCase(config.eid, eid) && dialCodes.nonEmpty
  }

}
