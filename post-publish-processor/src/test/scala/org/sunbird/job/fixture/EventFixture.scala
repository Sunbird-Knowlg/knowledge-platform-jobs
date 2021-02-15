package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1599417126869,"mid":"LP.1599417126869.b5a197ab-4111-4230-936b-39c1c67dbab0","actor":{"id":"Post Publish Processor","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"},"channel":"b00bc992ef25f1a9a8d63291e20efc8d","env":"sunbirddev"},"object":{"ver":"1599417119724","id":"do_11300581751853056018"},"edata":{"trackable":"{\"enabled\":\"Yes\",\"autoBatch\":\"Yes\"}","identifier":"do_11300581751853056018","createdFor":["ORG_001"],"createdBy":"874ed8a5-782e-4f6c-8f36-e0288455901e","name":"Origin Content","action":"post-publish-process","iteration":1,"id":"do_11300581751853056018","mimeType":"application/vnd.ekstep.content-collection","contentType":"TextBook","pkgVersion":19.0,"status":"Live"}}
      |""".stripMargin

  val QREVENT_1: String =
    """
      |{"eid": "BE_JOB_REQUEST","ets": 1613105152461,"mid": "LP.1613105152461.044714c5-a4db-4e98-9b52-609aa199cde6","actor": {"id": "Post Publish Processor","type": "System"},"context": {"pdata": {"ver": "1.0","id": "org.ekstep.platform"},"channel": "b00bc992ef25f1a9a8d63291e20efc8d","env": "sunbirddev"},"object": {"ver": "1613105149590","id": "do_113214556543234"},"edata": {"trackable": "{\"enabled\":\"No\",\"autoBatch\":\"No\"}","identifier": "do_113214556543234","createdFor": ["ORG_001"],"createdBy": "874ed8a5-782e-4f6c-8f36-e0288455901e","name": "FITB","action": "post-publish-process","iteration": 1,"id": "do_113214556543234","mimeType": "application/vnd.ekstep.content-collection","contentType": "Course","pkgVersion": 1,"status": "Live"}}
      |""".stripMargin

  val QREVENT_2: String =
    """
      |{"eid": "BE_JOB_REQUEST","ets": 1613105152461,"mid": "LP.1613105152461.044714c5-a4db-4e98-9b52-609aa199cde6","actor": {"id": "Post Publish Processor","type": "System"},"context": {"pdata": {"ver": "1.0","id": "org.ekstep.platform"},"channel": "b00bc992ef25f1a9a8d63291e20efc8d","env": "sunbirddev"},"object": {"ver": "1613105149590","id": "do_113214556543235"},"edata": {"trackable": "{\"enabled\":\"No\",\"autoBatch\":\"No\"}","identifier": "do_113214556543235","createdFor": ["ORG_001"],"createdBy": "874ed8a5-782e-4f6c-8f36-e0288455901e","name": "FITB","action": "post-publish-process","iteration": 1,"id": "do_113214556543235","mimeType": "application/vnd.ekstep.content-collection","contentType": "Course","pkgVersion": 1,"status": "Live"}}
      |""".stripMargin

  val QREVENT_3: String =
    """
      |{"eid": "BE_JOB_REQUEST","ets": 1613105152461,"mid": "LP.1613105152461.044714c5-a4db-4e98-9b52-609aa199cde6","actor": {"id": "Post Publish Processor","type": "System"},"context": {"pdata": {"ver": "1.0","id": "org.ekstep.platform"},"channel": "b00bc992ef25f1a9a8d63291e20efc8d","env": "sunbirddev"},"object": {"ver": "1613105149590","id": "do_113214556543236"},"edata": {"trackable": "{\"enabled\":\"No\",\"autoBatch\":\"No\"}","identifier": "do_113214556543236","createdFor": ["ORG_001"],"createdBy": "874ed8a5-782e-4f6c-8f36-e0288455901e","name": "FITB","action": "post-publish-process","iteration": 1,"id": "do_113214556543236","mimeType": "application/vnd.ekstep.content-collection","contentType": "Course","pkgVersion": 1,"status": "Live"}}
      |""".stripMargin

  val QREVENT_4: String =
    """
      |{"eid": "BE_JOB_REQUEST","ets": 1613105152461,"mid": "LP.1613105152461.044714c5-a4db-4e98-9b52-609aa199cde6","actor": {"id": "Post Publish Processor","type": "System"},"context": {"pdata": {"ver": "1.0","id": "org.ekstep.platform"},"channel": "b00bc992ef25f1a9a8d63291e20efc8d","env": "sunbirddev"},"object": {"ver": "1613105149590","id": "do_113214556543237"},"edata": {"trackable": "{\"enabled\":\"No\",\"autoBatch\":\"No\"}","identifier": "do_113214556543237","createdFor": ["ORG_001"],"createdBy": "874ed8a5-782e-4f6c-8f36-e0288455901e","name": "FITB","action": "post-publish-process","iteration": 1,"id": "do_113214556543237","mimeType": "application/vnd.ekstep.content-collection","contentType": "Course","pkgVersion": 1,"status": "Live"}}
      |""".stripMargin
}
