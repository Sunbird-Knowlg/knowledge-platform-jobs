package org.sunbird.job.migration.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1619527882745,"mid":"LP.1619527882745.32dc378a-430f-49f6-83b5-bd73b767ad36","actor":{"id":"cassandra-migration","type":"System"},"context":{"channel":"ORG_001","pdata":{"id":"org.sunbird.platform","ver":"1.0"},"env":"dev"},"object":{"id":"","ver":""},"edata":{"keyspace":"hierarchy_store","table":"content_hierarchy", "column": "hierarchy", "action":"migrate-cassandra","iteration":1}}
      |""".stripMargin

  val EVENT_2: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1619527882745,"mid":"LP.1619527882745.32dc378a-430f-49f6-83b5-bd73b767ad36","actor":{"id":"cassandra-migration","type":"System"},"context":{"channel":"ORG_001","pdata":{"id":"org.sunbird.platform","ver":"1.0"},"env":"dev"},"object":{"id":"","ver":""},"edata":{"keyspace":"dummy","table":"dummy", "column": "dummy", "action":"dummy","iteration":1}}
      |""".stripMargin
}