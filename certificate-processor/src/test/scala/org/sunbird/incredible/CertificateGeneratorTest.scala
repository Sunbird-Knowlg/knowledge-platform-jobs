package org.sunbird.incredible

import java.util
import org.mockito.ArgumentMatchers.{any, endsWith}
import org.mockito.Mockito.when
import org.sunbird.incredible.pojos.ob.{Criteria, Issuer, SignatoryExtension}
import org.sunbird.incredible.processor.CertModel
import org.sunbird.incredible.processor.signature.SignatureHelper

class CertificateGeneratorTest extends BaseTestSpec {


  override protected def beforeAll(): Unit = {

  }


  "check generate certificate" should "should return certificate extension" in {
    val map: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]()
    val issuerIn: Issuer = Issuer(context = "https://staging.sunbirded.org/certs/v1/context.json", name = "issuer name", url = "url")
    val signatory: SignatoryExtension = SignatoryExtension(context = "", identity = "CEO", name = "signatory name", designation = "CEO", image = "https://cdn.pixabay.com/photo/2014/11/09/08/06/signature-523237__340.jpg")
    val certModel: CertModel = CertModel(courseName = "java", recipientName = "Test", certificateName = "100PercentCompletionCertificate", issuedDate = "2019-08-21",
      issuer = issuerIn, signatoryList = Array(signatory), identifier = "8e57723e-4541-11eb-b378-0242ac130002", criteria = Criteria(narrative = "Course Completion"))
    val certificateGenerator = new CertificateGenerator(populatesPropertiesMap(map), "conf/")
    val certificateExtension = certificateGenerator.getCertificateExtension(certModel)
    certificateExtension.evidence.get.name shouldBe "java"
    certificateExtension.recipient.name shouldBe "Test"
  }

  "check generate certificate with signature" should "should return certificate extension" in {
    val mockHttpUtil: HttpUtil = mock[HttpUtil]
    SignatureHelper.httpUtil = mockHttpUtil
    when(mockHttpUtil.post(endsWith("/sign/256"), any[String])).thenReturn(HTTPResponse(200, """{"signatureValue":"Yul8hVc3+ShzEQU/u1og7f8b0xVf2N4WyMdFgELdz74dpfMWBhZ8snsvit3JKMptyA4JKSywqUoNeAMcEmtgurqAaS7oMwMulPJnvAsx2xDCOxq/UVPZGi63zPsItP2dTahLsEJQjPyQOEoEW5KW3oefRJO066Fr/L/Y5XNg2goDhvYHoHdAkpfr/IFsQqG0hWPzKglKOwd0R+LIuv13MIywBjYg9qY6cWs9BtTSMwXyayBhm6YkLgdb0LiBD/","keyId":"256"}"""))
    val map: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
    map.put(JsonKeys.KEYS, new util.HashMap[String, AnyRef]() {
      {
        put(JsonKeys.ID, "256")
      }
    })
    val issuerIn: Issuer = Issuer(context = "https://staging.sunbirded.org/certs/v1/context.json", name = "issuer name", url = "url")
    val signatory: SignatoryExtension = SignatoryExtension(context = "", identity = "CEO", name = "signatory name", designation = "CEO", image = "https://cdn.pixabay.com/photo/2014/11/09/08/06/signature-523237__340.jpg")
    val certModel: CertModel = CertModel(courseName = "java", recipientName = "Test", certificateName = "100PercentCompletionCertificate", issuedDate = "2019-08-21",
      issuer = issuerIn, signatoryList = Array(signatory), identifier = "8e57723e-4541-11eb-b378-0242ac130002", criteria = Criteria(narrative = "Course Completion"))
    val certificateGenerator = new CertificateGenerator(populatesPropertiesMap(map), "conf/")
    val certificateExtension = certificateGenerator.getCertificateExtension(certModel)
    certificateExtension.evidence.get.name shouldBe "java"
    certificateExtension.signature.get should not be null
    certificateExtension.signature.get.signatureValue should equal("Yul8hVc3+ShzEQU/u1og7f8b0xVf2N4WyMdFgELdz74dpfMWBhZ8snsvit3JKMptyA4JKSywqUoNeAMcEmtgurqAaS7oMwMulPJnvAsx2xDCOxq/UVPZGi63zPsItP2dTahLsEJQjPyQOEoEW5KW3oefRJO066Fr/L/Y5XNg2goDhvYHoHdAkpfr/IFsQqG0hWPzKglKOwd0R+LIuv13MIywBjYg9qY6cWs9BtTSMwXyayBhm6YkLgdb0LiBD/")
  }


}