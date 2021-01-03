package org.sunbird.incredible

import java.io.FileNotFoundException
import java.util

import org.sunbird.incredible.pojos.ob.{Criteria, Issuer, SignatoryExtension}
import org.sunbird.incredible.processor.CertModel
import org.sunbird.incredible.processor.views.SvgGenerator

class SvgGeneratorTest extends BaseTestSpec {


  "check generate encoded svg data" should "should return encoded string" in {
    val map: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]()
    val issuerIn: Issuer = Issuer(context = "https://staging.sunbirded.org/certs/v1/context.json", name = "issuer name", url = "url")
    val signatory: SignatoryExtension = SignatoryExtension(context = "", identity = "CEO", name = "signatory name", designation = "CEO", image = "https://cdn.pixabay.com/photo/2014/11/09/08/06/signature-523237__340.jpg")
    val certModel: CertModel = CertModel(courseName = "java", recipientName = "Test", certificateName = "100PercentCompletionCertificate", issuedDate = "2019-08-21",
      issuer = issuerIn, signatoryList = Array(signatory), identifier = "8e57723e-4541-11eb-b378-0242ac130002", criteria = Criteria(narrative = "Course Completion"))
    val certificateGenerator = new CertificateGenerator
    val certificateExtension = certificateGenerator.getCertificateExtension(getCertProperties(""),certModel)
    val printUri = SvgGenerator.generate(certificateExtension, "encodedQr", "https://sunbirdstagingpublic.blob.core.windows.net/sunbird-content-staging/content/template_svg_03_staging/artifact/cbse.svg")
    printUri should not be null
    printUri should startWith("data:image/svg+xml,")
  }

  "check svg generator" should "should throw exception" in {
    val map: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]()
    val issuerIn: Issuer = Issuer(context = "https://staging.sunbirded.org/certs/v1/context.json", name = "issuer name", url = "url")
    val signatory: SignatoryExtension = SignatoryExtension(context = "", identity = "CEO", name = "signatory name", designation = "CEO", image = "https://cdn.pixabay.com/photo/2014/11/09/08/06/signature-523237__340.jpg")
    val certModel: CertModel = CertModel(courseName = "java", recipientName = "Test", certificateName = "100PercentCompletionCertificate", issuedDate = "2019-08-21",
      issuer = issuerIn, signatoryList = Array(signatory), identifier = "8e57723e-4541-11eb-b378-0242ac130002", criteria = Criteria(narrative = "Course Completion"))
    val certificateGenerator = new CertificateGenerator
    val certificateExtension = certificateGenerator.getCertificateExtension(getCertProperties(null),certModel)
    intercept[FileNotFoundException] {
      val printUri = SvgGenerator.generate(certificateExtension, "encodedQr", "https://sunbirdstagingpublic.blob.core.windows.net/sunbird-content-staging/content/template_svg_03_staging/artifact/cbse.sv")
    }
  }
}
