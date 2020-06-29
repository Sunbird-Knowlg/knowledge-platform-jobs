package org.sunbird.kp.course.fixture

object EventFixture {

  val EVENT_WITH_MESSAGE_ID: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1563788371969,"mid":"LMS.1563788371969.590c5fa0-0ce8-46ed-bf6c-681c0a1fdac8","actor":{"type":"System","id":"Course Batch Updater"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}},"object":{"type":"CourseBatchEnrolment","id":"0126083288437637121_8454cb21-3ce9-4e30-85b5-fade097880d8"},"edata":{"contents":[{"contentId":"do_11260735471149056012299","status":2},{"contentId":"do_11260735471149056012300","status":1},{"contentId":"do_11260735471149056012301","status":1}],"action":"batch-enrolment-update","iteration":1,"batchId":"0126083288437637121","userId":"8454cb21-3ce9-4e30-85b5-fade097880d8","courseId":"do_1127212344324751361295"}}
      |""".stripMargin
}