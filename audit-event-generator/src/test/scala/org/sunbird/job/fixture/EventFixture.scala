package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"ets":1518517878987,"nodeUniqueId":"do_11243969846440755213","requestId":null,"transactionData":{"properties":{"code":{"ov":null,"nv":"test_code"},"keywords":{"ov":null,"nv":["colors","games"]},"channel":{"ov":null,"nv":"in.ekstep"},"language":{"ov":null,"nv":["English"]},"mimeType":{"ov":null,"nv":"application/pdf"},"idealScreenSize":{"ov":null,"nv":"normal"},"createdOn":{"ov":null,"nv":"2018-02-13T16:01:18.947+0530"},"contentDisposition":{"ov":null,"nv":"inline"},"contentEncoding":{"ov":null,"nv":"identity"},"lastUpdatedOn":{"ov":null,"nv":"2018-02-13T16:01:18.947+0530"},"contentType":{"ov":null,"nv":"Story"},"audience":{"ov":null,"nv":["Learner"]},"IL_SYS_NODE_TYPE":{"ov":null,"nv":"DATA_NODE"},"os":{"ov":null,"nv":["All"]},"visibility":{"ov":null,"nv":"Default"},"mediaType":{"ov":null,"nv":"content"},"osId":{"ov":null,"nv":"org.ekstep.quiz.app"},"versionKey":{"ov":null,"nv":"1518517878947"},"idealScreenDensity":{"ov":null,"nv":"hdpi"},"framework":{"ov":null,"nv":"NCF"},"compatibilityLevel":{"ov":null,"nv":1.0},"IL_FUNC_OBJECT_TYPE":{"ov":null,"nv":"Content"},"name":{"ov":null,"nv":"Untitled Resource"},"IL_UNIQUE_ID":{"ov":null,"nv":"do_11243969846440755213"},"status":{"ov":null,"nv":"Draft"},"resourceType":{"ov":null,"nv":"Story"}}},"operationType":"CREATE","nodeGraphId":113603,"label":"Untitled Resource","graphId":"domain","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2018-02-13T16:01:18.987+0530","objectType":"Content"}
      |""".stripMargin

  val EVENT_2: String =
    """
      |{"ets":1552464504681,"channel":"in.ekstep","transactionData":{"properties":{"lastStatusChangedOn":{"ov":"2019-03-13T13:25:43.129+0530","nv":"2019-03-13T13:38:24.358+0530"},"lastSubmittedOn":{"ov":null,"nv":"2019-03-13T13:38:21.901+0530"},"lastUpdatedOn":{"ov":"2019-03-13T13:36:20.093+0530","nv":"2019-03-13T13:38:24.399+0530"},"status":{"ov":"Draft","nv":"Review"},"versionKey":{"ov":"1552464380093","nv":"1552464504399"}}},"label":"Resource Content 1","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2019-03-13T13:38:24.680+0530","objectType":"Content","nodeUniqueId":"do_11271778298376192013","requestId":null,"operationType":"UPDATE","nodeGraphId":590883,"graphId":"domain"}
      |""".stripMargin

  val EVENT_3: String =
    """
      |{"ets":1552464380225,"channel":"in.ekstep","transactionData":{"properties":{"s3Key":{"ov":null,"nv":"content/do_11271778298376192013/artifact/pdf_1552464372724.pdf"},"size":{"ov":null,"nv":433994.0},"artifactUrl":{"ov":null,"nv":"https://ekstep-public-dev.s3-ap-south-1.amazonaws.com/content/do_11271778298376192013/artifact/pdf_1552464372724.pdf"},"lastUpdatedOn":{"ov":"2019-03-13T13:25:43.129+0530","nv":"2019-03-13T13:36:20.093+0530"},"versionKey":{"ov":"1552463743129","nv":"1552464380093"}}},"label":"Resource Content 1","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2019-03-13T13:36:20.223+0530","objectType":"Content","nodeUniqueId":"do_11271778298376192013","requestId":null,"operationType":"UPDATE","nodeGraphId":590883,"graphId":"domain"}
      |""".stripMargin

  val EVENT_4: String =
    """
      |{"ets":1552645516180,"channel":"in.ekstep","transactionData":{"properties":{"ownershipType":{"ov":null,"nv":["createdBy"]},"code":{"ov":null,"nv":"test.res.1"},"channel":{"ov":null,"nv":"in.ekstep"},"language":{"ov":null,"nv":["English"]},"mimeType":{"ov":null,"nv":"application/pdf"},"idealScreenSize":{"ov":null,"nv":"normal"},"createdOn":{"ov":null,"nv":"2019-03-15T15:55:16.071+0530"},"contentDisposition":{"ov":null,"nv":"inline"},"lastUpdatedOn":{"ov":null,"nv":"2019-03-15T15:55:16.071+0530"},"contentEncoding":{"ov":null,"nv":"identity"},"dialcodeRequired":{"ov":null,"nv":"No"},"contentType":{"ov":null,"nv":"Resource"},"lastStatusChangedOn":{"ov":null,"nv":"2019-03-15T15:55:16.071+0530"},"audience":{"ov":null,"nv":["Learner"]},"IL_SYS_NODE_TYPE":{"ov":null,"nv":"DATA_NODE"},"visibility":{"ov":null,"nv":"Default"},"os":{"ov":null,"nv":["All"]},"mediaType":{"ov":null,"nv":"content"},"osId":{"ov":null,"nv":"org.ekstep.quiz.app"},"versionKey":{"ov":null,"nv":"1552645516071"},"idealScreenDensity":{"ov":null,"nv":"hdpi"},"framework":{"ov":null,"nv":"NCF"},"compatibilityLevel":{"ov":null,"nv":1.0},"IL_FUNC_OBJECT_TYPE":{"ov":null,"nv":"Content"},"name":{"ov":null,"nv":"Resource Content 1"},"IL_UNIQUE_ID":{"ov":null,"nv":"do_11271927206783385611"},"status":{"ov":null,"nv":"Draft"}}},"mid":"9ea9ae7a-9cc1-493d-aac3-3c66cd9ff01b","label":"Resource Content 1","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2019-03-15T15:55:16.178+0530","objectType":"Content","nodeUniqueId":"do_11271927206783385611","requestId":null,"operationType":"CREATE","nodeGraphId":590921,"graphId":"domain"}
      |""".stripMargin

  val EVENT_5: String =
    """
      |{"ets":1.614227716965E12,"transactionData":{"removedTags":[],"addedRelations":[{"rel":"hasSequenceMember","id":"do_11322375329257062411","label":"event 4.1: 11-1","dir":"OUT","type":"Event","relMetadata":{"IL_SEQUENCE_INDEX":1.0}}],"removedRelations":[],"addedTags":[],"properties":{}},"mid":"fc0bb006-7269-4b10-96c3-10672fca53a0","label":"eventset 4","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2021-02-25T04:35:16.965+0000","objectType":"EventSet","partition":1,"nodeUniqueId":"do_11322375344215654413","operationType":"UPDATE","nodeGraphId":509461.0,"graphId":"domain"}
      |""".stripMargin

  val EVENT_6: String =
    """
      |{"ets":1502102183388,"nodeUniqueId":"do_112276071067320320114","requestId":null,"transactionData":{"addedRelations":[{"rel":"hasSequenceMember","id":"do_1123032073439723521148","label":"Test unit 11","dir":"IN","type":"Content"}],"removedRelations":[],"properties":{"name":{"nv":"","ov":""}}},"operationType":"CREATE","nodeGraphId":105631,"label":"collaborator test","graphId":"domain","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2017-08-07T10:36:23.388+0000","objectType":"Content"}
      |""".stripMargin

  val EVENT_7: String =
    """
      |{"ets":1614776853781,"channel":"b00bc992ef25f1a9a8d63291e20efc8d","transactionData":{"properties":{"dialcodes":{"ov":null,"nv":["K1W6L6"]}}},"mid":"5b5633a2-3c18-49a6-8822-6d7b85338104","label":"Test Again","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2021-03-03T13:07:33.781+0000","objectType":"Collection","nodeUniqueId":"do_1132282511204024321262","requestId":null,"operationType":"UPDATE","nodeGraphId":510086,"graphId":"domain"}
      |""".stripMargin
}