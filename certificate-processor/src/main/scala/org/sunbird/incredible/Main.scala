package org.sunbird.incredible

import java.io.{File, FileOutputStream, IOException}
import java.util
import java.util.Base64

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.sunbird.incredible.pojos.ob.{CertificateExtension, Criteria, Issuer, SignatoryExtension}
import org.sunbird.incredible.processor.CertModel
import org.sunbird.incredible.processor.views.SvgGenerator

object Main {
  val fos = new FileOutputStream(new File("file.txt"))
  Console.withOut(fos)()

  def main(args: Array[String]): Unit = {

    val map: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]()
    val issuer: Issuer = Issuer(context = "", id = "id", name = "issuer name", url = "url")
    val signatory: SignatoryExtension = SignatoryExtension(context = "", identity = "iden", name = "signatory name", designation = "CEO", image = "e")
    val certModel: CertModel = CertModel(courseName = "java", recipientName = "Aishwarya", recipientId = "identi", recipientPhone = "recipentPhone", recipientEmail = "email", certificateName = "completion", certificateDescription = "desc cert", issuedDate = "2020-08-09",
      issuer = issuer, validFrom = "12-12-2020", signatoryList = Array(signatory), certificateLogo = "sdsh", assessedOn = "", expiry = "12-12-2020", identifier = "ide", criteria = Criteria(narrative = "21312"))
    val certificateGenerator = new CertificateGenerator(populatesPropertiesMap(map), "conf/")

    val certificateExtension: CertificateExtension = certificateGenerator.getCertificateExtension(certModel)
    println("c" + certificateExtension)
    val qrMap: Map[String, Any] = certificateGenerator.generateQrCode()
    val file: File = qrMap(JsonKeys.QR_CODE_FILE).asInstanceOf[java.io.File]
    val encodedQrCode: String = encodeQrCode(file)
     val printuti = SvgGenerator.generate(certificateExtension, encodedQrCode,"https://sunbirdstagingpublic.blob.core.windows.net/sunbird-content-staging/content/template_svg_03_staging/artifact/cbse.svg" )
    println(printuti)
  }


  @throws[IOException]
  private def encodeQrCode(file: File): String = {
    val fileContent = FileUtils.readFileToByteArray(file)
    file.delete
    Base64.getEncoder.encodeToString(fileContent)
  }


  private def populatesPropertiesMap(req: util.Map[String, AnyRef]): Map[String, String] = {
    val basePath: String = req.getOrDefault(req.get(JsonKeys.BASE_PATH), "http://localhost:9000").asInstanceOf[String]
    val tag: String = "12323434"
    val keysObject: util.Map[String, AnyRef] = req.get(JsonKeys.KEYS).asInstanceOf[util.Map[String, AnyRef]]
    //    if (MapUtils.isNotEmpty(keysObject)) {
    //      val keyId: String = keysObject.get(JsonKeys.ID).asInstanceOf[String]
    //      properties.put(JsonKeys.KEY_ID, keyId)
    //      properties.put(JsonKeys.SIGN_CREATOR, basePath.concat("/").concat(keyId).concat("http:"))
    //      properties.put(JsonKeys.PUBLIC_KEY_URL, basePath.concat("/").concat(JsonKeys.KEYS).concat("/").concat(keyId).concat(config.PUBLIC_KEY_URL))
    //    }
    val properties: Map[String, String] = Map(JsonKeys.BASE_PATH -> basePath, JsonKeys.TAG -> tag,
      JsonKeys.BADGE_URL -> basePath.concat(if (StringUtils.isNotBlank(tag)) "/".concat(tag).concat(JsonKeys.BADGE_URL) else JsonKeys.BADGE_URL),
      JsonKeys.ISSUER_URL -> basePath.concat(JsonKeys.ISSUER_URL), JsonKeys.EVIDENCE_URL -> basePath.concat(JsonKeys.EVIDENCE_URL),
      JsonKeys.CONTEXT -> basePath.concat(JsonKeys.CONTEXT), JsonKeys.ACCESS_CODE_LENGTH -> "6", JsonKeys.ENC_SERVICE_URL -> "", JsonKeys.SIGNATORY_EXTENSION -> String.format("%s/%s/%s", basePath, JsonKeys.SIGNATORY_EXTENSION, "context.json"))

    properties
  }

}
