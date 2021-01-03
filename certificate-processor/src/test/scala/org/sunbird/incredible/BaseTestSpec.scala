package org.sunbird.incredible


import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class BaseTestSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {


  implicit val certificateConfig: CertificateConfig = CertificateConfig(basePath = "http://localhost:9000", encryptionServiceUrl = "http://localhost:8013", contextUrl = "context.json", evidenceUrl = JsonKeys.EVIDENCE_URL, issuerUrl = JsonKeys.ISSUER_URL, signatoryExtension = JsonKeys.SIGNATORY_EXTENSION)


  def getCertProperties(keyId: String): CertificateProperties = {
    val tag: String = "0131685518070087685"
    CertificateProperties(tag, keyId = keyId)
  }

}
