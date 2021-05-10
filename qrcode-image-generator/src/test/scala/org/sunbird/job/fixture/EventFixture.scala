package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"eid":"BE_QR_IMAGE_GENERATOR","processId":"e101e2cf-d592-4095-89fe-38acc0b8b276","objectId":"do_2132430402739240961409","dialcodes":[{"data":"https://preprod.ntp.net.in/dial/N3X6Y3","text":"N3X6Y3","id":"2_N3X6Y3"},{"data":"https://preprod.ntp.net.in/dial/U3J1J9","text":"U3J1J9","id":"0_U3J1J9"},{"data":"https://preprod.ntp.net.in/dial/J9Z5R4","text":"J9Z5R4","id":"1_J9Z5R4"}],"storage":{"container":"dial","path":"01269878797503692810/","fileName":"do_2132430402739240961409_english_class_10_english_1616582317028"},"config":{"errorCorrectionLevel":"H","pixelsPerBlock":2,"qrCodeMargin":3,"textFontName":"Verdana","textFontSize":11,"textCharacterSpacing":0.1,"imageFormat":"png","colourModel":"Grayscale","imageBorderSize":1}}
      |""".stripMargin
//
//  val EVENT_2: String =
//    """
//      |{"ets":1502102183388,"nodeUniqueId":"do_112276071067320320114","requestId":null,"transactionData":{"addedRelations":[{"rel":"hasSequenceMember","id":"do_1123032073439723521148","label":"Test unit 11","dir":"IN","type":"Content"}],"removedRelations":[],"properties":{"name":{"nv":"","ov":""}}},"operationType":"CREATE","nodeGraphId":105631,"label":"collaborator test","graphId":"domain","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2017-08-07T10:36:23.388+0000","objectType":"Content"}
//      |""".stripMargin
//
//  val EVENT_3: String =
//    """
//      |{"ets":1615191835547,"channel":"01309282781705830427","transactionData":{"removedTags":[],"addedRelations":[],"removedRelations":[{"rel":"associatedTo","id":"do_113198273083662336127","label":"qq\n","dir":"OUT","type":"AssessmentItem","relMetadata":{}}],"addedTags":[],"properties":{}},"mid":"98145983-63dc-4d55-866c-248d49306ad8","label":"ECML_CHANGES","nodeType":"DATA_NODE","userId":"5a587cc1-e018-4859-a0a8-e842650b9d64","createdOn":"2021-03-08T08:23:55.547+0000","objectType":"Content","nodeUniqueId":"do_1132316371218268161118","requestId":null,"operationType":"UPDATE","nodeGraphId":510477,"graphId":"domain"}
//      |""".stripMargin
//
//  val EVENT_4: String =
//    """
//      |{"ets":1500888709490,"requestId":null,"transactionData":{"properties":{"IL_SYS_NODE_TYPE":{"ov":null,"nv":"DATA_NODE"},"morphology":{"ov":null,"nv":false},"consumerId":{"ov":null,"nv":"a6654129-b58d-4dd8-9cf2-f8f3c2f458bc"},"channel":{"ov":null,"nv":"in.ekstep"},"lemma":{"ov":null,"nv":"ವಿಶ್ಲೇಷಣೆ"},"createdOn":{"ov":null,"nv":"2017-07-24T09:32:18.130+0000"},"versionKey":{"ov":null,"nv":"1500888738130"},"IL_FUNC_OBJECT_TYPE":{"ov":null,"nv":"Word"},"ekstepWordnet":{"ov":null,"nv":false},"lastUpdatedOn":{"ov":null,"nv":"2017-07-24T09:32:18.130+0000"},"isPhrase":{"ov":null,"nv":false},"IL_UNIQUE_ID":{"ov":null,"nv":"ka_11229528054276096015"},"status":{"ov":null,"nv":"Draft"}}},"nodeGraphId":433342,"label":"ವಿಶ್ಲೇಷಣೆ","graphId":"ka","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2017-07-24T09:31:49.490+0000","objectType":"Word"}
//      |""".stripMargin

}