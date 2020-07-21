package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"actor":{"id":"Post Publish Processor", "type":"System"}, "eid":"BE_JOB_REQUEST", "edata":{"createdFor":["ORG_001"], "createdBy":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8", "name":"Untitled Course", "action":"publish-shallow-content", "iteration":1.0, "id":"do_11305855864948326411234", "mimeType":"application/vnd.ekstep.content-collection", "contentType":"Course", "pkgVersion":7.0, "status":"Live"}, "partition":0, "ets":"1.593769627322E12", "context":{"pdata":{"ver":1.0, "id":"org.ekstep.platform"}, "channel":"b00bc992ef25f1a9a8d63291e20efc8d", "env":"sunbirddev"}, "mid":"LP.1593769627322.459a018c-5ec3-4c11-96c1-cd84d3786b85", "object":{"ver":"1593769626118", "id":"do_11305855864948326411234"}}
      |""".stripMargin
  val EVENT_2: String =
    """
      |{"actor":{"id":"Post Publish Processor", "type":"System"}, "eid":"BE_JOB_REQUEST", "edata":{"createdFor":["ORG_001"], "createdBy":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8", "name":"Untitled Course", "action":"publish-shallow-content", "iteration":1.0, "id":"KP_FT_1594149835504", "mimeType":"application/vnd.ekstep.content-collection", "contentType":"Course", "pkgVersion":7.0, "status":"Live"}, "partition":0, "ets":"1.593769627322E12", "context":{"pdata":{"ver":1.0, "id":"org.ekstep.platform"}, "channel":"b00bc992ef25f1a9a8d63291e20efc8d", "env":"sunbirddev"}, "mid":"LP.1593769627322.459a018c-5ec3-4c11-96c1-cd84d3786b85", "object":{"ver":"1593769626118", "id":"KP_FT_1594149835504"}}
      |""".stripMargin
}