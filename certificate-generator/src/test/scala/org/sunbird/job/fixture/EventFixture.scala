package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1563788371969,"mid":"LMS.1563788371969.590c5fa0-0ce8-46ed-bf6c-681c0a1fdac8","actor":{"type":"System","id":"Certificate Generator"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}},"object":{"type":"GenerateCertificate","id":"874ed8a5-782e-4f6c-8f36-e0288455901e"},"edata":{"svgTemplate":"template-url.svg","courseName":"new course may23","issuedDate":"2019-08-21","data":[{"recipientName":"Creation ","recipientId":"874ed8a5-782e-4f6c-8f36-e0288455901e"}],"name":"100PercentCompletionCertificate","tag":"0125450863553740809","issuer":{"name":"Gujarat Council of Educational Research and Training","url":"https://gcert.gujarat.gov.in/gcert/"},"orgId":"ORG_001","signatoryList":[{"name":"CEO Gujarat","id":"CEO","designation":"CEO","image":"https://cdn.pixabay.com/photo/2014/11/09/08/06/signature-523237__340.jpg"}],"keys":{"id":"5768"},"criteria":{"narrative":"course completion certificate"},"basePath":"https://{{domain_name}}/certs"}}
      |""".stripMargin
}

