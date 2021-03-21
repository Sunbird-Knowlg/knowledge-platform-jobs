package org.sunbird.job.fixture

object EventFixture {

  val DATA_NODE_CREATE: String =
    """
      |{"ets":1614346609623,"channel":"channel-1","transactionData":{"properties":{"ownershipType":{"ov":null,"nv":["createdBy"]},"code":{"ov":null,"nv":"org.sunbird.zf7fcK"},"credentials":{"ov":null,"nv":"{\"enabled\":\"No\"}"},"subject":{"ov":null,"nv":["Geography"]},"channel":{"ov":null,"nv":"channel-1"},"language":{"ov":null,"nv":["English"]},"mimeType":{"ov":null,"nv":"application/vnd.ekstep.content-collection"},"idealScreenSize":{"ov":null,"nv":"normal"},"createdOn":{"ov":null,"nv":"2021-02-26T13:36:49.592+0000"},"primaryCategory":{"ov":null,"nv":"Digital Textbook"},"contentDisposition":{"ov":null,"nv":"inline"},"additionalCategories":{"ov":null,"nv":["Textbook"]},"lastUpdatedOn":{"ov":null,"nv":"2021-02-26T13:36:49.592+0000"},"contentEncoding":{"ov":null,"nv":"gzip"},"dialcodeRequired":{"ov":null,"nv":"No"},"contentType":{"ov":null,"nv":"TextBook"},"trackable":{"ov":null,"nv":"{\"enabled\":\"No\",\"autoBatch\":\"No\"}"},"subjectIds":{"ov":null,"nv":["ncf_subject_geography"]},"lastStatusChangedOn":{"ov":null,"nv":"2021-02-26T13:36:49.592+0000"},"audience":{"ov":null,"nv":["Student"]},"IL_SYS_NODE_TYPE":{"ov":null,"nv":"DATA_NODE"},"os":{"ov":null,"nv":["All"]},"visibility":{"ov":null,"nv":"Default"},"consumerId":{"ov":null,"nv":"7411b6bd-89f3-40ec-98d1-229dc64ce77d"},"mediaType":{"ov":null,"nv":"content"},"osId":{"ov":null,"nv":"org.ekstep.quiz.app"},"version":{"ov":null,"nv":2},"versionKey":{"ov":null,"nv":"1614346609592"},"idealScreenDensity":{"ov":null,"nv":"hdpi"},"license":{"ov":null,"nv":"CC BY-SA 4.0"},"framework":{"ov":null,"nv":"NCF"},"createdBy":{"ov":null,"nv":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8"},"compatibilityLevel":{"ov":null,"nv":1},"IL_FUNC_OBJECT_TYPE":{"ov":null,"nv":"Collection"},"userConsent":{"ov":null,"nv":"Yes"},"name":{"ov":null,"nv":"Test"},"IL_UNIQUE_ID":{"ov":null,"nv":"do_1132247274257203201191"},"status":{"ov":null,"nv":"Draft"}}},"mid":"62d07bca-ac01-4faf-8042-af25979bd902","label":"Test","nodeType":"DATA_NODE","userId":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8","createdOn":"2021-02-26T13:36:49.623+0000","objectType":"Collection","nodeUniqueId":"do_1132247274257203201191","requestId":null,"operationType":"CREATE","nodeGraphId":509674,"graphId":"domain"}
      |""".stripMargin

  val DATA_NODE_UPDATE: String =
    """
      |{"ets":1614346671374,"channel":"channel-1","transactionData":{"properties":{"description":{"ov":null,"nv":"updated description"},"language":{"ov":["English"],"nv":null},"lastUpdatedOn":{"ov":"2021-02-26T13:36:49.592+0000","nv":"2021-02-26T13:37:51.285+0000"},"versionKey":{"ov":"1614346609592","nv":"1614346671285"}}},"mid":"6395492e-8ecc-4ad0-a6a6-bf524b8d2ac0","label":"Test","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2021-02-26T13:37:51.373+0000","objectType":"Collection","nodeUniqueId":"do_1132247274257203201191","requestId":null,"operationType":"UPDATE","nodeGraphId":509674,"graphId":"domain"}
      |""".stripMargin


  val DATA_NODE_DELETE: String =
    """
      |{"ets":1614346963589,"channel":"channel-1","transactionData":{"properties":{"ownershipType":{"ov":["createdBy"],"nv":null},"code":{"ov":"org.sunbird.zf7fcK","nv":null},"credentials":{"ov":"{\"enabled\":\"No\"}","nv":null},"subject":{"ov":["Geography"],"nv":null},"channel":{"ov":"channel-1","nv":null},"description":{"ov":"updated description","nv":null},"language":{"ov":["English"],"nv":null},"mimeType":{"ov":"application/vnd.ekstep.content-collection","nv":null},"idealScreenSize":{"ov":"normal","nv":null},"createdOn":{"ov":"2021-02-26T13:36:49.592+0000","nv":null},"primaryCategory":{"ov":"Digital Textbook","nv":null},"additionalCategories":{"ov":["Textbook"],"nv":null},"contentDisposition":{"ov":"inline","nv":null},"contentEncoding":{"ov":"gzip","nv":null},"lastUpdatedOn":{"ov":"2021-02-26T13:37:51.285+0000","nv":null},"contentType":{"ov":"TextBook","nv":null},"dialcodeRequired":{"ov":"No","nv":null},"trackable":{"ov":"{\"enabled\":\"No\",\"autoBatch\":\"No\"}","nv":null},"audience":{"ov":["Student"],"nv":null},"subjectIds":{"ov":["ncf_subject_geography"],"nv":null},"lastStatusChangedOn":{"ov":"2021-02-26T13:36:49.592+0000","nv":null},"visibility":{"ov":"Default","nv":null},"os":{"ov":["All"],"nv":null},"IL_SYS_NODE_TYPE":{"ov":"DATA_NODE","nv":null},"consumerId":{"ov":"7411b6bd-89f3-40ec-98d1-229dc64ce77d","nv":null},"mediaType":{"ov":"content","nv":null},"osId":{"ov":"org.ekstep.quiz.app","nv":null},"version":{"ov":2,"nv":null},"versionKey":{"ov":"1614346671285","nv":null},"license":{"ov":"CC BY-SA 4.0","nv":null},"idealScreenDensity":{"ov":"hdpi","nv":null},"framework":{"ov":"NCF","nv":null},"createdBy":{"ov":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8","nv":null},"compatibilityLevel":{"ov":1,"nv":null},"userConsent":{"ov":"Yes","nv":null},"IL_FUNC_OBJECT_TYPE":{"ov":"Collection","nv":null},"name":{"ov":"Test","nv":null},"IL_UNIQUE_ID":{"ov":"do_1132247274257203201191","nv":null},"status":{"ov":"Draft","nv":null}}},"mid":"9759b9a4-3b07-473b-b38b-53cdce944288","label":"Test","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2021-02-26T13:42:43.589+0000","objectType":"Collection","nodeUniqueId":"do_1132247274257203201191","requestId":null,"operationType":"DELETE","nodeGraphId":509674,"graphId":"domain"}
      |""".stripMargin

  val DATA_NODE_UNKNOWN: String =
    """
      |{"ets":1614346609623,"channel":"channel-1","transactionData":{"properties":{"ownershipType":{"ov":null,"nv":["createdBy"]},"code":{"ov":null,"nv":"org.sunbird.zf7fcK"},"credentials":{"ov":null,"nv":"{\"enabled\":\"No\"}"},"subject":{"ov":null,"nv":["Geography"]},"channel":{"ov":null,"nv":"channel-1"},"language":{"ov":null,"nv":["English"]},"mimeType":{"ov":null,"nv":"application/vnd.ekstep.content-collection"},"idealScreenSize":{"ov":null,"nv":"normal"},"createdOn":{"ov":null,"nv":"2021-02-26T13:36:49.592+0000"},"primaryCategory":{"ov":null,"nv":"Digital Textbook"},"contentDisposition":{"ov":null,"nv":"inline"},"additionalCategories":{"ov":null,"nv":["Textbook"]},"lastUpdatedOn":{"ov":null,"nv":"2021-02-26T13:36:49.592+0000"},"contentEncoding":{"ov":null,"nv":"gzip"},"dialcodeRequired":{"ov":null,"nv":"No"},"contentType":{"ov":null,"nv":"TextBook"},"trackable":{"ov":null,"nv":"{\"enabled\":\"No\",\"autoBatch\":\"No\"}"},"subjectIds":{"ov":null,"nv":["ncf_subject_geography"]},"lastStatusChangedOn":{"ov":null,"nv":"2021-02-26T13:36:49.592+0000"},"audience":{"ov":null,"nv":["Student"]},"IL_SYS_NODE_TYPE":{"ov":null,"nv":"DATA_NODE"},"os":{"ov":null,"nv":["All"]},"visibility":{"ov":null,"nv":"Default"},"consumerId":{"ov":null,"nv":"7411b6bd-89f3-40ec-98d1-229dc64ce77d"},"mediaType":{"ov":null,"nv":"content"},"osId":{"ov":null,"nv":"org.ekstep.quiz.app"},"version":{"ov":null,"nv":2},"versionKey":{"ov":null,"nv":"1614346609592"},"idealScreenDensity":{"ov":null,"nv":"hdpi"},"license":{"ov":null,"nv":"CC BY-SA 4.0"},"framework":{"ov":null,"nv":"NCF"},"createdBy":{"ov":null,"nv":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8"},"compatibilityLevel":{"ov":null,"nv":1},"IL_FUNC_OBJECT_TYPE":{"ov":null,"nv":"Collection"},"userConsent":{"ov":null,"nv":"Yes"},"name":{"ov":null,"nv":"Test"},"IL_UNIQUE_ID":{"ov":null,"nv":"do_1132247274257203201191"},"status":{"ov":null,"nv":"Draft"}}},"mid":"62d07bca-ac01-4faf-8042-af25979bd902","label":"Test","nodeType":"DATA_NODE","userId":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8","createdOn":"2021-02-26T13:36:49.623+0000","objectType":"Collection","nodeUniqueId":"do_1132247274257203201191","requestId":null,"operationType":"UNKNOWN","nodeGraphId":509674,"graphId":"domain"}
      |""".stripMargin

  val DATA_NODE_CREATE_WITH_RELATION: String =
    """
      |{"ets":1502102183388,"nodeUniqueId":"do_112276071067320320114","requestId":null,"transactionData":{"removedTags":[],"addedRelations":[{"rel":"hasSequenceMember","id":"do_1123032073439723521148","label":"Test unit 11","dir":"IN","type":"Content"}],"removedRelations":[],"addedTags":[],"properties":{}},"operationType":"CREATE","nodeGraphId":105631,"label":"collaborator test","graphId":"domain","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2017-08-07T10:36:23.388+0000","objectType":"Content"}
      |""".stripMargin

  val DATA_NODE_UPDATE_WITH_RELATION: String =
    """
      |{"ets":1502102183388,"nodeUniqueId":"do_112276071067320320114","requestId":null,"transactionData":{"removedTags":[],"addedRelations":[],"removedRelations":[{"rel":"hasSequenceMember","id":"do_1123032073439723521148","label":"Test unit 11","dir":"IN","type":"Content"}],"addedTags":[],"properties":{}},"operationType":"UPDATE","nodeGraphId":105631,"label":"collaborator test","graphId":"domain","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2017-08-07T10:36:23.388+0000","objectType":"Content"}
      |""".stripMargin

  val DIALCODE_METRIC_CREATE: String =
    """
      |{"ets":1543561000015,"nodeUniqueId":"QR1234","transactionData":{"properties":{"average_scans_per_day":{"nv":2},"last_scan":{"nv":1541456052000},"total_dial_scans_local":{"nv":25},"first_scan":{"nv":1540469152000}}},"objectType":"","operationType":"CREATE","nodeType":"DIALCODE_METRICS","graphId":"domain","nodeGraphId":0}
      |""".stripMargin

  val DIALCODE_METRIC_UPDATE: String =
    """
      |{"ets":1543561000015,"nodeUniqueId":"QR1234","transactionData":{"properties":{"average_scans_per_day":{"nv":2},"last_scan":{"nv":1541456052000},"total_dial_scans_global":{"nv":25},"total_dial_scans_local":{"nv":null},"first_scan":{"nv":1540469152000}}},"objectType":"","operationType":"UPDATE","nodeType":"DIALCODE_METRICS","graphId":"domain","nodeGraphId":0}
      |""".stripMargin

  val DIALCODE_METRIC_DELETE: String =
    """
      |{"ets":1543561000015,"nodeUniqueId":"QR1234","transactionData":{"properties":{"average_scans_per_day":{"nv":null},"last_scan":{"nv":null},"total_dial_scans_local":{"nv":null},"first_scan":{"nv":null}}},"objectType":"","operationType":"DELETE","nodeType":"DIALCODE_METRICS","graphId":"domain","nodeGraphId":0}
      |""".stripMargin

  val DIALCODE_METRIC_UNKNOWN: String =
    """
      |{"ets":1543561000015,"nodeUniqueId":"QR1234","transactionData":{"properties":{"average_scans_per_day":{"nv":2},"last_scan":{"nv":1541456052000},"total_dial_scans_local":{"nv":25},"first_scan":{"nv":1540469152000}}},"objectType":"","operationType":"UNKNOWN","nodeType":"DIALCODE_METRICS","graphId":"domain","nodeGraphId":0}
      |""".stripMargin

  val DIALCODE_EXTERNAL_CREATE: String =
    """
      |{"nodeUniqueId":"X8R3W4","ets":1613072768797,"requestId":null,"audit":false,"transactionData":{"properties":{"dialcode_index":{"ov":null,"nv":9071809.0},"identifier":{"ov":null,"nv":"X8R3W4"},"channel":{"ov":null,"nv":"channelTest"},"batchcode":{"ov":null,"nv":"testPub0001.20210212T011555"},"publisher":{"ov":null,"nv":"testPub0001"},"generated_on":{"ov":null,"nv":"2021-02-12T01:16:07.750+0530"},"status":{"ov":null,"nv":"Draft"}}},"channel":"in.ekstep","index":true,"operationType":"CREATE","nodeType":"EXTERNAL","userId":"ANONYMOUS","createdOn":"2021-02-12T01:16:08.797+0530","objectType":"DialCode"}
      |""".stripMargin

  val DIALCODE_EXTERNAL_UPDATE: String =
    """
      |{"nodeUniqueId":"X8R3W4","ets":1613072768797,"requestId":null,"audit":false,"transactionData":{"properties":{"dialcode_index":{"ov":null,"nv":9071809.0},"identifier":{"ov":null,"nv":"X8R3W4"},"channel":{"ov":null,"nv":"channelTest Updated"},"batchcode":{"ov":"testPub0001.20210212T011555","nv":null},"publisher":{"ov":null,"nv":"testPub0001"},"generated_on":{"ov":null,"nv":"2021-02-12T01:16:07.750+0530"},"status":{"ov":null,"nv":"Draft"}}},"channel":"in.ekstep","index":true,"operationType":"UPDATE","nodeType":"EXTERNAL","userId":"ANONYMOUS","createdOn":"2021-02-12T01:16:08.797+0530","objectType":"DialCode"}
      |""".stripMargin

  val DIALCODE_EXTERNAL_DELETE: String =
    """
      |{"nodeUniqueId":"X8R3W4","ets":1613072768797,"requestId":null,"audit":false,"transactionData":{"properties":{"dialcode_index":{"nv":null},"identifier":{"nv":null},"channel":{"nv":null},"batchcode":{"nv":null},"publisher":{"nv":null},"generated_on":{"nv":null},"status":{"nv":null}}},"channel":"in.ekstep","index":true,"operationType":"DELETE","nodeType":"EXTERNAL","userId":"ANONYMOUS","createdOn":"2021-02-12T01:16:08.797+0530","objectType":"DialCode"}
      |""".stripMargin

  val DIALCODE_EXTERNAL_UNKNOWN: String =
    """
      |{"nodeUniqueId":"X8R3W4","ets":1613072768797,"requestId":null,"audit":false,"transactionData":{"properties":{"dialcode_index":{"ov":null,"nv":9071809.0},"identifier":{"ov":null,"nv":"X8R3W4"},"channel":{"ov":null,"nv":"channelTest"},"batchcode":{"ov":null,"nv":"testPub0001.20210212T011555"},"publisher":{"ov":null,"nv":"testPub0001"},"generated_on":{"ov":null,"nv":"2021-02-12T01:16:07.750+0530"},"status":{"ov":null,"nv":"Draft"}}},"channel":"in.ekstep","index":true,"operationType":"UNKNOWN","nodeType":"EXTERNAL","userId":"ANONYMOUS","createdOn":"2021-02-12T01:16:08.797+0530","objectType":"DialCode"}
      |""".stripMargin

  val UNKNOWN_NODE_TYPE: String =
    """
      |{"nodeUniqueId":"X8R3W4","ets":1613072768797,"requestId":null,"audit":false,"transactionData":{"properties":{"dialcode_index":{"ov":null,"nv":9071809.0},"identifier":{"ov":null,"nv":"X8R3W4"},"channel":{"ov":null,"nv":"channelTest"},"batchcode":{"ov":null,"nv":"testPub0001.20210212T011555"},"publisher":{"ov":null,"nv":"testPub0001"},"generated_on":{"ov":null,"nv":"2021-02-12T01:16:07.750+0530"},"status":{"ov":null,"nv":"Draft"}}},"channel":"in.ekstep","index":true,"operationType":"CREATE","nodeType":"UNKNOWN","userId":"ANONYMOUS","createdOn":"2021-02-12T01:16:08.797+0530","objectType":"DialCode"}
      |""".stripMargin

  val INDEX_FALSE: String =
    """
      |{"nodeUniqueId":"X8R3W4","ets":1613072768797,"requestId":null,"audit":false,"transactionData":{"properties":{"dialcode_index":{"ov":null,"nv":9071809.0},"identifier":{"ov":null,"nv":"X8R3W4"},"channel":{"ov":null,"nv":"channelTest"},"batchcode":{"ov":null,"nv":"testPub0001.20210212T011555"},"publisher":{"ov":null,"nv":"testPub0001"},"generated_on":{"ov":null,"nv":"2021-02-12T01:16:07.750+0530"},"status":{"ov":null,"nv":"Draft"}}},"channel":"in.ekstep","index":false,"operationType":"UNKNOWN","nodeType":"EXTERNAL","userId":"ANONYMOUS","createdOn":"2021-02-12T01:16:08.797+0530","objectType":"DialCode"}
      |""".stripMargin

  val DATA_NODE_FAILED: String =
    """
      |{"ets":1614346609623,"channel":"channel-1","transactionData":{"properties":{"ownershipType":{"ov":null,"nv":["createdBy"]},"code":{"ov":null,"nv":"org.sunbird.zf7fcK"},"credentials":{"ov":null,"nv":"{\"enabled\":\"No\"}"},"subject":{"ov":null,"nv":["Geography"]},"channel":{"ov":null,"nv":"channel-1"},"language":{"ov":null,"nv":["English"]},"mimeType":{"ov":null,"nv":"application/vnd.ekstep.content-collection"},"idealScreenSize":{"ov":null,"nv":"normal"},"createdOn":{"ov":null,"nv":"2021-02-26T13:36:49.592+0000"},"primaryCategory":{"ov":null,"nv":"Digital Textbook"},"contentDisposition":{"ov":null,"nv":"inline"},"additionalCategories":{"ov":null,"nv":["Textbook"]},"lastUpdatedOn":{"ov":null,"nv":"2021-02-26T13:36:49.592+0000"},"contentEncoding":{"ov":null,"nv":"gzip"},"dialcodeRequired":{"ov":null,"nv":"No"},"contentType":{"ov":null,"nv":"TextBook"},"trackable":{"ov":null,"nv":"{\"enabled\":\"No\",\"autoBatch\":\"No\"}"},"subjectIds":{"ov":null,"nv":["ncf_subject_geography"]},"lastStatusChangedOn":{"ov":null,"nv":"2021-02-26T13:36:49.592+0000"},"audience":{"ov":null,"nv":["Student"]},"IL_SYS_NODE_TYPE":{"ov":null,"nv":"DATA_NODE"},"os":{"ov":null,"nv":["All"]},"visibility":{"ov":null,"nv":"Default"},"consumerId":{"ov":null,"nv":"7411b6bd-89f3-40ec-98d1-229dc64ce77d"},"mediaType":{"ov":null,"nv":"content"},"osId":{"ov":null,"nv":"org.ekstep.quiz.app"},"version":{"ov":null,"nv":2},"versionKey":{"ov":null,"nv":"1614346609592"},"idealScreenDensity":{"ov":null,"nv":"hdpi"},"license":{"ov":null,"nv":"CC BY-SA 4.0"},"framework":{"ov":null,"nv":"NCF"},"createdBy":{"ov":null,"nv":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8"},"compatibilityLevel":{"ov":null,"nv":1},"IL_FUNC_OBJECT_TYPE":{"ov":null,"nv":"Collection"},"userConsent":{"ov":null,"nv":"Yes"},"name":{"ov":null,"nv":"Test"},"IL_UNIQUE_ID":{"ov":null,"nv":"do_1132247274257203201191"},"status":{"ov":null,"nv":"Draft"}}},"mid":"62d07bca-ac01-4faf-8042-af25979bd902","label":"Test","nodeType":"DATA_NODE","userId":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8","createdOn":"2021-02-26T13:36:49.623+0000","objectType":"UNKNOWN","nodeUniqueId":"do_1132247274257203201191","requestId":null,"operationType":"CREATE","nodeGraphId":509674,"graphId":"domain"}
      |""".stripMargin

}