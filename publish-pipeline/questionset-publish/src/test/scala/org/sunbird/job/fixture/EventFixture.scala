package org.sunbird.job.fixture

object EventFixture {

  val QUESTION_EVENT1: String =
  """
    |{"eid":"BE_JOB_REQUEST","ets":1609926636251,"mid":"LP.1609926636251.b93d8562-537e-4e52-bcf5-b9175a550391","actor":{"id":"question-publish","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":""},"object":{"ver":"1609926299686","id":"do_113188615625731"},"edata":{"publish_type":"public","metadata":{"identifier":"do_113188615625731","mimeType":"application/vnd.sunbird.question","lastPublishedBy":null,"pkgVersion":1,"objectType":"Question"},"action":"publish","iteration":1}}
    |""".stripMargin

}
