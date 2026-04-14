package org.sunbird.job.fixture

object EventFixture {

  val PDF_EVENT1: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1619527882745,"mid":"LP.1619527882745.32dc378a-430f-49f6-83b5-bd73b767ad36","actor":{"id":"content-publish","type":"System"},"context":{"channel":"","pdata":{"id":"org.sunbird.platform","ver":"1.0"}},"object":{"id":"do_11329603741667328018","ver":"1619153418829"},"edata":{"publish_type":"public","metadata":{"identifier":"do_11329603741667328018","mimeType":"application/pdf","objectType":"Content","lastPublishedBy":"sample-last-published-by","pkgVersion":1},"action":"publish","iteration":1}}
      |""".stripMargin

  val QUESTIONSET_EVENT1: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1609926636251,"mid":"LP.1609926636251.b93d8562-537e-4e52-bcf5-b9175a550391","actor":{"id":"content-publish","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":"","requestId":"req-123","featureName":"questionset-publish"},"object":{"ver":"1609926299686","id":"do_113188615625730"},"edata":{"publish_type":"public","metadata":{"identifier":"do_113188615625730","mimeType":"application/vnd.sunbird.questionset","lastPublishedBy":"test-user","pkgVersion":1,"objectType":"QuestionSet"},"action":"publish","iteration":1}}
      |""".stripMargin

  val QUESTION_EVENT1: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1609926636251,"mid":"LP.1609926636251.b93d8562-537e-4e52-bcf5-b9175a550391","actor":{"id":"content-publish","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":"","requestId":"req-124","featureName":"question-publish"},"object":{"ver":"1609926299686","id":"do_113188615625731"},"edata":{"publish_type":"public","metadata":{"identifier":"do_113188615625731","mimeType":"application/vnd.sunbird.question","lastPublishedBy":"test-user","pkgVersion":1,"objectType":"Question"},"action":"publish","iteration":1}}
      |""".stripMargin
}
