package org.sunbird.job.fixture

object EventFixture {

  val PUBLISH_CHAIN_EVENT: String =
  """
    |{"eid": "BE_JOB_REQUEST","ets": 1634812181709,"mid": "LP.1634812181709.88b311c2-a733-4846-b2a2-31d9dceda436","actor": {"id": "interactivevideo-publish","type": "System"},
    |"context": {"pdata": {"ver": "1.0","id": "org.sunbird.platform"},"channel": "0133866213417451520","env": "dev"},"object": {"ver": "1634810156009","id": "do_213392364484722688135"},"edata": {"publish_type": "public",
    |"metadata": {},"publishchain": [{"identifier": "do identifier of questionset 1","mimeType": "application/vnd.sunbird.questionset","lastPublishedBy": "2ca3f787-379f-42d4-9cc3-6bc03a2d4cca","pkgVersion": null,"objectType": "QuestionSet","state": "Live","publishErr": "","order": 1},{"identifier": "do identifier of questionset 2","mimeType": "application/vnd.sunbird.questionset","lastPublishedBy": "2ca3f787-379f-42d4-9cc3-6bc03a2d4cca","pkgVersion": null,"objectType": "QuestionSet","state": "Live","publishErr": "","order": 2},],"action": "publishchain","iteration": 1,"contentType": "Resource"}}
    |""".stripMargin

}
