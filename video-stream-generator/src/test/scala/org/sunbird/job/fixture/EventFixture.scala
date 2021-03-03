package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1598956686981,"mid":"LP.1598956686981.a260af12-cd9b-4ffd-a525-1d944df47c61","actor":{"id":"Post Publish Processor","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"},"channel":"sunbird","env":"sunbirddev"},"object":{"ver":"1587632475439","id":"do_3126597193576939521910"},"edata":{"action":"post-publish-process","iteration":1,"identifier":"do_3126597193576939521910","channel":"01254290140407398431","artifactUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/assets/do_1126980548391075841140/ariel-view-of-earth.mp4","mimeType":"video/mp4","contentType":"Resource","pkgVersion":1,"status":"Live"}}
      |""".stripMargin

  val EVENT_2: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1598956686981,"mid":"LP.1598956686981.a260af12-cd9b-4ffd-a525-1d944df47c61","actor":{"id":"Post Publish Processor","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"},"channel":"sunbird","env":"sunbirddev"},"object":{"ver":"1587632475439","id":"do_3126597193576939521910"},"edata":{"action":"post-publish-process","iteration":1,"identifier":"do_3126597193576939521910","channel":"01254290140407398431","artifactUrl":"https://sunbirded.com/test.mp4","mimeType":"video","contentType":"Resource","pkgVersion":1,"status":"Live"}}
      |""".stripMargin
}