package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{"object":{"id":"do_112806963140329472124"},"index":"true","ets":"1591949601174","eventData":{"action":"update-es-index","stage":1, "identifier":"do_112806963140329472124","channel":"in.ekstep","language":["English"],"name":"Resource Content 1","status":"Draft","level1Concept":["Addition"],"level1Name":["Math-Magic"],"textbook_name":["How Many Times?"],"sourceURL":"https://dev.sunbirded.org/play/content/do_112806963140329472124","source":["Sunbird 1"]}}
      |""".stripMargin

  val EVENT_2: String =
    """
      |{"object":{"id":"do_112806963140329472124"},"index":"false","ets":"1591949601174","eventData":{"action":"update-es-index","stage":1,"identifier":"do_112806963140329472124","channel":"in.ekstep","language":["English"],"name":"Resource Content 1","status":"Draft","level1Concept":["Addition"],"level1Name":["Math-Magic"],"textbook_name":["How Many Times?"],"sourceURL":"https://dev.sunbirded.org/play/content/do_112806963140329472124"}}
      |""".stripMargin

}