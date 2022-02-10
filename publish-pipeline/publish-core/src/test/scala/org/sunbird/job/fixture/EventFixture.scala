package org.sunbird.job.fixture

object EventFixture {

  val PUBLISH_CHAIN_EVENT : String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1619527882745,"mid":"LP.1619527882745.32dc378a-430f-49f6-83b5-bd73b767ad36","actor":{"id":"questionset-publish","type":"System"},"context":{"channel":"Sunbird","pdata":{"id":"org.sunbird.platform","ver":"3.0"},"env":"dev"},"object":{"id":"do_11345039506718720014","ver":"1637732551528"},"edata":{"publish_type":"public","metadata":{},"publishchain":[{"identifier":"do_11343549820647014411","mimeType":"application/vnd.sunbird.questionset","objectType":"QuestionSet","lastPublishedBy":"","pkgVersion":2,"state":"Processing"},{"identifier":"do_11343556733536665611","mimeType":"application/vnd.sunbird.questionset","objectType":"QuestionSet","lastPublishedBy":"","pkgVersion":2,"state":"Processing"},{"identifier":"do_11345176856416256012","mimeType":"video/mp4","objectType":"Content","lastPublishedBy":"","pkgVersion":2,"state":"Processing"}],"action":"publishchain","iteration":1}}
      |""".stripMargin


}
