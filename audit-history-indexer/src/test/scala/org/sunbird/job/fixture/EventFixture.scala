package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"ets":1518517878987,"nodeUniqueId":"do_11243969846440755213","requestId":null,"transactionData":{"properties":{"code":{"ov":null,"nv":"test_code"},"keywords":{"ov":null,"nv":["colors","games"]},"channel":{"ov":null,"nv":"in.ekstep"},"language":{"ov":null,"nv":["English"]},"mimeType":{"ov":null,"nv":"application/pdf"},"idealScreenSize":{"ov":null,"nv":"normal"},"createdOn":{"ov":null,"nv":"2018-02-13T16:01:18.947+0530"},"contentDisposition":{"ov":null,"nv":"inline"},"contentEncoding":{"ov":null,"nv":"identity"},"lastUpdatedOn":{"ov":null,"nv":"2018-02-13T16:01:18.947+0530"},"contentType":{"ov":null,"nv":"Story"},"audience":{"ov":null,"nv":["Learner"]},"IL_SYS_NODE_TYPE":{"ov":null,"nv":"DATA_NODE"},"os":{"ov":null,"nv":["All"]},"visibility":{"ov":null,"nv":"Default"},"mediaType":{"ov":null,"nv":"content"},"osId":{"ov":null,"nv":"org.ekstep.quiz.app"},"versionKey":{"ov":null,"nv":"1518517878947"},"idealScreenDensity":{"ov":null,"nv":"hdpi"},"framework":{"ov":null,"nv":"NCF"},"compatibilityLevel":{"ov":null,"nv":1.0},"IL_FUNC_OBJECT_TYPE":{"ov":null,"nv":"Content"},"name":{"ov":null,"nv":"Untitled Resource"},"IL_UNIQUE_ID":{"ov":null,"nv":"do_11243969846440755213"},"status":{"ov":null,"nv":"Draft"},"resourceType":{"ov":null,"nv":"Story"}}},"operationType":"CREATE","nodeGraphId":113603,"label":"Untitled Resource","graphId":"domain","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2018-02-13T16:01:18.987+0530","objectType":"Content"}
      |""".stripMargin

  val EVENT_2: String =
    """
      |{"ets":1502102183388,"nodeUniqueId":"do_112276071067320320114","requestId":null,"transactionData":{"addedRelations":[{"rel":"hasSequenceMember","id":"do_1123032073439723521148","label":"Test unit 11","dir":"IN","type":"Content"}],"removedRelations":[],"properties":{"name":{"nv":"","ov":""}}},"operationType":"CREATE","nodeGraphId":105631,"label":"collaborator test","graphId":"domain","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2017-08-07T10:36:23.388+0000","objectType":"Content"}
      |""".stripMargin

  val EVENT_3: String =
    """
      |{"ets":1615191835547,"channel":"01309282781705830427","transactionData":{"removedTags":[],"addedRelations":[],"removedRelations":[{"rel":"associatedTo","id":"do_113198273083662336127","label":"qq\n","dir":"OUT","type":"AssessmentItem","relMetadata":{}}],"addedTags":[],"properties":{}},"mid":"98145983-63dc-4d55-866c-248d49306ad8","label":"ECML_CHANGES","nodeType":"DATA_NODE","userId":"5a587cc1-e018-4859-a0a8-e842650b9d64","createdOn":"2021-03-08T08:23:55.547+0000","objectType":"Content","nodeUniqueId":"do_1132316371218268161118","requestId":null,"operationType":"UPDATE","nodeGraphId":510477,"graphId":"domain"}
      |""".stripMargin

  val EVENT_4: String =
    """
      |{"ets":1552645516180,"channel":"in.ekstep","transactionData":{"properties":{"ownershipType":{"ov":null,"nv":["createdBy"]},"code":{"ov":null,"nv":"test.res.1"},"channel":{"ov":null,"nv":"in.ekstep"},"language":{"ov":null,"nv":["English"]},"mimeType":{"ov":null,"nv":"application/pdf"},"idealScreenSize":{"ov":null,"nv":"normal"},"createdOn":{"ov":null,"nv":"2019-03-15T15:55:16.071+0530"},"contentDisposition":{"ov":null,"nv":"inline"},"lastUpdatedOn":{"ov":null,"nv":"2019-03-15T15:55:16.071+0530"},"contentEncoding":{"ov":null,"nv":"identity"},"dialcodeRequired":{"ov":null,"nv":"No"},"contentType":{"ov":null,"nv":"Resource"},"lastStatusChangedOn":{"ov":null,"nv":"2019-03-15T15:55:16.071+0530"},"audience":{"ov":null,"nv":["Learner"]},"IL_SYS_NODE_TYPE":{"ov":null,"nv":"DATA_NODE"},"visibility":{"ov":null,"nv":"Default"},"os":{"ov":null,"nv":["All"]},"mediaType":{"ov":null,"nv":"content"},"osId":{"ov":null,"nv":"org.ekstep.quiz.app"},"versionKey":{"ov":null,"nv":"1552645516071"},"idealScreenDensity":{"ov":null,"nv":"hdpi"},"framework":{"ov":null,"nv":"NCF"},"compatibilityLevel":{"ov":null,"nv":1.0},"IL_FUNC_OBJECT_TYPE":{"ov":null,"nv":"Content"},"name":{"ov":null,"nv":"Resource Content 1"},"IL_UNIQUE_ID":{"ov":null,"nv":"do_11271927206783385611"},"status":{"ov":null,"nv":"Draft"}}},"mid":"9ea9ae7a-9cc1-493d-aac3-3c66cd9ff01b","label":"Resource Content 1","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2019-03-15T15:55:16.178+0530","objectType":"Content","nodeUniqueId":"do_11271927206783385611","requestId":null,"operationType":"CREATE","nodeGraphId":590921,"graphId":"domain"}
      |""".stripMargin

  val EVENT_5: String =
    """
      |{"ets":1502102183388,"nodeUniqueId":"do_112276071067320320114","requestId":null,"transactionData":{"addedRelations":[{"rel":"hasSequenceMember","id":"do_1123032073439723521148","label":"Test unit 11","dir":"IN","type":"Content"}],"removedRelations":[],"properties":{"name":{"nv":"","ov":""}}},"operationType":"CREATE","nodeGraphId":105631,"label":"collaborator test","graphId":"domain","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2017-08-07T10:36:23.388+0000","objectType":"Content"}
      |""".stripMargin
}