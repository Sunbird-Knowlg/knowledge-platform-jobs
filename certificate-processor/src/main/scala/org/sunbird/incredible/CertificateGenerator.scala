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

case class QrCodeModel(accessCode: String, qrFile: File)

class CertificateGenerator(implicit certificateConfig: CertificateConfig) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[CertificateGenerator])

  @throws[SignatureException.UnreachableException]
  @throws[InvalidDateFormatException]
  @throws[SignatureException.CreationException]
  @throws[IOException]
  def getCertificateExtension(properties: Map[String, String], certModel: CertModel): CertificateExtension = {
    val certificateExtension = new CertificateFactory(properties).createCertificate(certModel)
    certificateExtension
  }

  def getUUID(certificateExtension: CertificateExtension): String = {
    var idStr: String = null
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


  private def checkDirectoryExists(directory: String): Unit = {
    val file = new File(directory)
    if (!file.exists) {
      logger.info("File directory does not exist." + file.getName)
      file.mkdirs
    }
  }

  def generateQrCode(uuid: String, directory: String, basePath: String): QrCodeModel = {
    checkDirectoryExists(directory)
    val accessCodeGenerator: AccessCodeGenerator = new AccessCodeGenerator
    val accessCode = accessCodeGenerator.generate()
    val qrCodeGenerationModel = QRCodeGenerationModel(text = accessCode, fileName = directory + uuid, data = basePath + "/" + uuid)
    val qrCodeImageGenerator = new QRCodeImageGenerator()
    val qrCodeFile = qrCodeImageGenerator.createQRImages(qrCodeGenerationModel)
    QrCodeModel(accessCode, qrCodeFile)
  }

}
