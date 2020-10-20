package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1599417126869,"mid":"LP.1599417126869.b5a197ab-4111-4230-936b-39c1c67dbab0","actor":{"id":"Post Publish Processor","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"},"channel":"b00bc992ef25f1a9a8d63291e20efc8d","env":"sunbirddev"},"object":{"ver":"1599417119724","id":"do_11300581751853056018"},"edata":{"trackable":"{\"enabled\":\"Yes\",\"autoBatch\":\"Yes\"}","identifier":"do_11300581751853056018","createdFor":["ORG_001"],"createdBy":"874ed8a5-782e-4f6c-8f36-e0288455901e","name":"Origin Content","action":"post-publish-process","iteration":1,"id":"do_11300581751853056018","mimeType":"application/vnd.ekstep.content-collection","contentType":"TextBook","pkgVersion":19.0,"status":"Live"}}
      |""".stripMargin

}
