package org.sunbird.incredible.processor.qrcode


import org.sunbird.incredible.{BaseTestSpec}

class AccessCodeGeneratorTest extends BaseTestSpec {


  "check accessCode generator" should "should return  accessCode" in {
    val accessCodeGenerator: AccessCodeGenerator = new AccessCodeGenerator
    val code: String = accessCodeGenerator.generate(6)
    code.length should be(6)
  }

}