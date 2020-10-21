package org.sunbird.job.fixture

object EventFixture {

  val EVENT_1: String =
    """
      |{ "actor": { "id": "Certificate Post Processor", "type": "System" }, "eid": "BE_JOB_REQUEST", "edata": { "action": "post-process-certificate", "iteration": 1,"templateId":"template_01_dev_001", "batchId": "0131000245281587206", "userId": "user001", "courseId": "do_11309999837886054415", "certificate": { "id": "certificateId", "name": "Course merit certificate", "token": "P4L3Y9", "lastIssuedOn": "2019-08-21"}}, "ets": 1564144562948, "context": { "pdata": { "ver": "1.0", "id": "org.sunbird.platform" } }, "mid": "LP.1564144562948.0deae013-378e-4d3b-ac16-237e3fc6149a", "object": { "id": "batchId_courseId", "type": "CourseCertificatePostProcessor" }}
      |""".stripMargin
}