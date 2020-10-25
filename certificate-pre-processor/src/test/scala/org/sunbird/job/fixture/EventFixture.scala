package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{ "actor": {
      |    "id": "Course Certificate Pre Processor",
      |    "type": "System"
      |  },
      |  "eid": "BE_JOB_REQUEST",,"edata":{"action":"issue-certificate","userIds":["95e4942d-cbe8-477d-aebd-ad8e6de4bfc8"],"courseId":"do_11305984881537024012255","batchId":"0130598559365038081","reIssue":true}, "ets": 1564144562948,
      |  "context": {
      |    "pdata": {
      |      "ver": "1.0",
      |      "id": "org.sunbird.platform"
      |    }
      |  },
      |  "mid": "LP.1564144562948.0deae013-378e-4d3b-ac16-237e3fc6149a",
      |  "object": {
      |    "id": "batchId_courseId",
      |    "type": "CourseCertificatePreProcessor"
      |  }}
      |""".stripMargin

  val EVENT_2: String =
    """
      |{ "actor": {
      |    "id": "Course Certificate Pre Processor",
      |    "type": "System"
      |  },
      |  "eid": "BE_JOB_REQUEST",,"edata":{"action":"issue-certificate","userIds":["95e4942d-cbe8-477d-aebd-ad8e6de4bfc8"],"courseId":"do_11305984881537024012255","batchId":"0130598559365038081","reIssue":true}, "ets": 1564144562948,
      |  "context": {
      |    "pdata": {
      |      "ver": "1.0",
      |      "id": "org.sunbird.platform"
      |    }
      |  },
      |  "mid": "LP.1564144562948.0deae013-378e-4d3b-ac16-237e3fc6149a",
      |  "object": {
      |    "id": "batchId_courseId",
      |    "type": "CourseCertificatePreProcessor"
      |  }}
      |""".stripMargin

}
