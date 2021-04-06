package org.sunbird.job.fixture

object EventFixture {

  val IMAGE_ASSET: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1615443994411,"mid":"LP.1615443994411.c94524f2-0d45-4da7-93d8-6d4059f6f999","actor":{"id":"Asset Enrichment Samza Job","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":"b00bc992ef25f1a9a8d63291e20efc8d","env":"dev"},"object":{"ver":"1615443994322","id":"do_113233716442578944194"},"edata":{"action":"assetenrichment","iteration":1,"mediaType":"image","status":"Processing","objectType":"Asset"}}
      |""".stripMargin

  val VIDEO_MP4_ASSET: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1615444029534,"mid":"LP.1615444029534.a4588b0e-50e9-4af7-8a73-2efa8efc68aa","actor":{"id":"Asset Enrichment Samza Job","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":"b00bc992ef25f1a9a8d63291e20efc8d","env":"dev"},"object":{"ver":"1551877993917","id":"do_1127129845261680641588"},"edata":{"action":"assetenrichment","iteration":1,"mediaType":"video","status":"Processing","objectType":"Asset"}}
      |""".stripMargin

  val VIDEO_YOUTUBE_ASSET: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1615444029534,"mid":"LP.1615444029534.a4588b0e-50e9-4af7-8a73-2efa8efc68aa","actor":{"id":"Asset Enrichment Samza Job","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":"b00bc992ef25f1a9a8d63291e20efc8d","env":"dev"},"object":{"ver":"1551877993917","id":"do_1127129845261680641599"},"edata":{"action":"assetenrichment","iteration":1,"mediaType":"video","status":"Processing","objectType":"Asset"}}
      |""".stripMargin

}
