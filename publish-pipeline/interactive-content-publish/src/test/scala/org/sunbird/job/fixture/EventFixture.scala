package org.sunbird.job.fixture

object EventFixture {

  val PDF_EVENT1: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1619527882745,"mid":"LP.1619527882745.32dc378a-430f-49f6-83b5-bd73b767ad36","actor":{"id":"content-publish","type":"System"},"context":{"channel":"","pdata":{"id":"org.sunbird.platform","ver":"1.0"}},"object":{"id":"do_11329603741667328018","ver":"1619153418829"},"edata":{"publish_type":"public","metadata":{"identifier":"do_11329603741667328018","mimeType":"application/pdf","objectType":"Content","lastPublishedBy":"sample-last-published-by","pkgVersion":1},"action":"publish","iteration":1}}
      |""".stripMargin

  val PUBLISH_CHAIN_EVENT : String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1619527882745,"mid":"LP.1619527882745.32dc378a-430f-49f6-83b5-bd73b767ad36","actor":{"id":"questionset-publish","type":"System"},"context":{"channel":"Sunbird","pdata":{"id":"org.sunbird.platform","ver":"3.0"},"env":"dev"},"object":{"id":"do_11345039506718720014","ver":"1637732551528"},"edata":{"publish_type":"public","metadata":{},"publishchain":[{"identifier":"do_11343549820647014411","mimeType":"application/vnd.sunbird.questionset","objectType":"QuestionSet","lastPublishedBy":"","pkgVersion":2,"state":"Processing"},{"identifier":"do_11343556733536665611","mimeType":"application/vnd.sunbird.questionset","objectType":"QuestionSet","lastPublishedBy":"","pkgVersion":2,"state":"Processing"},{"identifier":"do_11345176856416256012","mimeType":"video/mp4","objectType":"Content","lastPublishedBy":"","pkgVersion":2,"state":"Processing"}],"action":"publishchain","iteration":1}}
      |""".stripMargin

  val PUBLISH_CHAIN_EVENT_WITH_INVALID_TYPE : String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1619527882745,"mid":"LP.1619527882745.32dc378a-430f-49f6-83b5-bd73b767ad36","actor":{"id":"questionset-publish","type":"System"},"context":{"channel":"Sunbird","pdata":{"id":"org.sunbird.platform","ver":"3.0"},"env":"dev"},"object":{"id":"do_11345039506718720014","ver":"1637732551528"},"edata":{"publish_type":"public","metadata":{},"publishchain":[{"identifier":"do_11343549820647014411","mimeType":"application/vnd.sunbird.questionset","objectType":"ABC","lastPublishedBy":"","pkgVersion":2,"state":"Processing"}],"action":"publishchain","iteration":1}}
      |""".stripMargin


  val PUBLISH_EVENT_WITH_PUBLISH_STRING : String =
    """
{
      |  "eid": "BE_JOB_REQUEST",
      |  "ets": 1619527882745,
      |  "mid": "LP.1619527882745.32dc378a-430f-49f6-83b5-bd73b767ad36",
      |  "actor": {
      |    "id": "questionset-publish",
      |    "type": "System"
      |  },
      |  "context": {
      |    "channel": "Sunbird",
      |    "pdata": {
      |      "id": "org.sunbird.platform",
      |      "ver": "3.0"
      |    },
      |    "env": "dev"
      |  },
      |  "object": {
      |    "id": "do_11345039506718720014",
      |    "ver": "1637732551528"
      |  },
      |  "edata": {
      |    "publish_type": "public",
      |    "metadata": {
      |      "identifier":"do_11343549820647014411",
      |      "mimeType":"video/mp4",
      |      "objectType":"Content",
      |      "lastPublishedBy":"",
      |      "pkgVersion":2,
      |      "publishChainMetadata":"{\n  \"eid\": \"BE_JOB_REQUEST\",\n  \"ets\": 1619527882745,\n  \"mid\": \"LP.1619527882745.32dc378a-430f-49f6-83b5-bd73b767ad36\",\n  \"actor\": {\n    \"id\": \"questionset-publish\",\n    \"type\": \"System\"\n  },\n  \"context\": {\n    \"channel\": \"Sunbird\",\n    \"pdata\": {\n      \"id\": \"org.sunbird.platform\",\n      \"ver\": \"3.0\"\n    },\n    \"env\": \"dev\"\n  },\n  \"object\": {\n    \"id\": \"do_11345039506718720014\",\n    \"ver\": \"1637732551528\"\n  },\n  \"edata\": {\n    \"publish_type\": \"public\",\n    \"metadata\": {},\n    \"publishchain\": [\n      {\n        \"identifier\": \"do_11343549820647014411\",\n        \"mimeType\": \"application\/vnd.sunbird.questionset\",\n        \"objectType\": \"QuestionSet\",\n        \"lastPublishedBy\": \"\",\n        \"pkgVersion\": 2,\n        \"state\": \"Processing\"\n      },\n      {\n        \"identifier\": \"do_11343556733536665611\",\n        \"mimeType\": \"application\/vnd.sunbird.questionset\",\n        \"objectType\": \"QuestionSet\",\n        \"lastPublishedBy\": \"\",\n        \"pkgVersion\": 2,\n        \"state\": \"Processing\"\n      },\n      {\n        \"identifier\": \"do_11345176856416256012\",\n        \"mimeType\": \"video\/mp4\",\n        \"objectType\": \"Content\",\n        \"lastPublishedBy\": \"\",\n        \"pkgVersion\": 2,\n        \"state\": \"Processing\"\n      }\n    ],\n    \"action\": \"publish\",\n    \"iteration\": 1\n  }\n}"
      |    },
      |    "action": "publish",
      |    "iteration": 1
      |  }
      |}      |""".stripMargin
}
