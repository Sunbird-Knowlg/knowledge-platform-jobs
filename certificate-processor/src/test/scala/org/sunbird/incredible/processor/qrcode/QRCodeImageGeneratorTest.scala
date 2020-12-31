package org.sunbird.incredible.processor.qrcode

import org.sunbird.incredible.{BaseTestSpec, CertificateGenerator, QrCodeModel}
class QRCodeImageGeneratorTest extends BaseTestSpec {



  "check qrCode generator" should "should return qrCode file" in {
    val certificateGenerator = new CertificateGenerator()
    val qrCodeModel: QrCodeModel = certificateGenerator.generateQrCode("8e57723e-4541-11eb-b378-0242ac130002", "certificates/","http://localhost:9000")
    qrCodeModel.accessCode.length should be(6)
    qrCodeModel.qrFile.exists() should be(true)
  }


}
