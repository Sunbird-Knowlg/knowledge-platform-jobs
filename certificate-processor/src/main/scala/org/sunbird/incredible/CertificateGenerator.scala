package org.sunbird.incredible

import java.io.{File, IOException}
import java.net.{URI, URISyntaxException}

import org.apache.commons.lang.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import org.sunbird.incredible.pojos.exceptions.InvalidDateFormatException
import org.sunbird.incredible.pojos.ob.CertificateExtension
import org.sunbird.incredible.processor.CertModel
import org.sunbird.incredible.processor.qrcode.{AccessCodeGenerator, QRCodeGenerationModel, QRCodeImageGenerator}
import org.sunbird.incredible.processor.signature.SignatureException

class CertificateGenerator(private var properties: Map[String, String],
                           private var directory: String) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[CertificateGenerator])

  var certificateExtension: CertificateExtension = _

  @throws[SignatureException#UnreachableException]
  @throws[InvalidDateFormatException]
  @throws[SignatureException#CreationException]
  @throws[IOException]
  def getCertificateExtension(certModel: CertModel): CertificateExtension = {
    certificateExtension = new CertificateFactory(properties).createCertificate(certModel)
    certificateExtension
  }


  def getUUID(certificateExtension: CertificateExtension): String = {
    var idStr: String = _
    try {
      val uri = new URI(certificateExtension.id)
      val path = uri.getPath
      idStr = path.substring(path.lastIndexOf('/') + 1)
    } catch {
      case e: URISyntaxException =>
        logger.info("")
    }
    StringUtils.substringBefore(idStr, ".")
  }


  private def checkDirectoryExists(): Unit = {
    val file = new File(directory)
    if (!file.exists) {
      logger.info("File directory does not exist." + file.getName)
      file.mkdirs
    }
  }

  def generateQrCode(): Map[String, Any] = {
    checkDirectoryExists()
    val uuid: String = getUUID(certificateExtension)
    val accessCodeGenerator: AccessCodeGenerator = new AccessCodeGenerator(properties.get(JsonKeys.ACCESS_CODE_LENGTH).map(_.toDouble).get)
    val accessCode = accessCodeGenerator.generate()
    val qrCodeGenerationModel = QRCodeGenerationModel(text = accessCode, fileName = directory + uuid, data = properties.get(JsonKeys.BASE_PATH) + "/" + uuid)
    val qrCodeImageGenerator = new QRCodeImageGenerator()
    val qrCodeFile = qrCodeImageGenerator.createQRImages(qrCodeGenerationModel)
    val qrMap = Map(JsonKeys.QR_CODE_FILE -> qrCodeFile, JsonKeys.ACCESS_CODE -> accessCode)
    logger.info("Qrcode {} is created for the certificate", qrCodeFile.getName)
    qrMap
  }

}
