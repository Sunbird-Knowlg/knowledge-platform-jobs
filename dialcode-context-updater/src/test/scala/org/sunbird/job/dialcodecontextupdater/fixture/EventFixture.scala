package org.sunbird.job.dialcodecontextupdater.fixture

object EventFixture {

  val DIALCODE_EVENT: String =
    """{"eid":"BE_JOB_REQUEST","ets":1648720639981,"mid":"LP.1648720639981.d6b1d8c8-7a4a-483a-b83a-b752bede648c","actor":{"id":"DIALcodecontextupdateJob","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":"01269878797503692810","env":"dev"},"object":{"ver":"1.0","id":"0117CH01"},"edata":{"action":"dialcode-context-update","iteration":1,"dialcode":"0117CH01","identifier":"d0_1234","traceId":"2342345345"}}""".stripMargin
}
