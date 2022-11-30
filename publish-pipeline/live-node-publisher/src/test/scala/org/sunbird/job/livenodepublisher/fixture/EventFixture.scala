package org.sunbird.job.livenodepublisher.fixture

object EventFixture {

  val PDF_EVENT1: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1619527882745,"mid":"LP.1619527882745.32dc378a-430f-49f6-83b5-bd73b767ad36","actor":{"id":"content-publish","type":"System"},"context":{"channel":"","pdata":{"id":"org.sunbird.platform","ver":"1.0"}},"object":{"id":"do_11329603741667328018","ver":"1619153418829"},"edata":{"publish_type":"public","metadata":{"identifier":"do_11329603741667328018","mimeType":"application/pdf","objectType":"Content","lastPublishedBy":"sample-last-published-by","pkgVersion":1},"action":"publish","iteration":1}}
      |""".stripMargin
}
