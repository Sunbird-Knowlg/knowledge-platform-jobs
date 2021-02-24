package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"ets":1.614170753568E12,"channel":"01309282781705830427","transactionData":{"properties":{"lastUpdatedOn":{"ov":"2021-02-24T12:22:30.737+0000","nv":"2021-02-24T12:45:53.479+0000"},"languageCode":{"nv":["en"]},"versionKey":{"ov":"1614169350737","nv":"1614170753479"}}},"mid":"9b919f91-0b75-44a5-9051-364a9a5a9fd9","label":"Test Question Paper","nodeType":"DATA_NODE","userId":"ANONYMOUS","createdOn":"2021-02-24T12:45:53.568+0000","objectType":"Collection","partition":3,"nodeUniqueId":"do_113223271204044800137","operationType":"UPDATE","nodeGraphId":509918.0,"graphId":"domain"}
      |""".stripMargin

}