package org.sunbird.kp.course.fixture

object EventFixture {

  /**
   * case-1. Inserting a first course which is having 3 leaf nodes in the redis database and cassandra database
   * does not contains this course id.
   * courseId =
   *
   * Expect:
   *          1 - Since it's first course it should generate START Event of the course
   *          2 - Progress of this course should be 30% since it has only one subject is completed and remaining are yet to finish)
   *          3 - It should update the
   *                  content-status = Map(do_11260735471149056012300 -> 1,do_11260735471149056012299 ->2, do_11260735471149056012300 -> 1),
   *                  completionpercentage = 30,
   *                  status = 1,
   *                  progress = 1(1 content finished out of 3) into the cassandra
   */

  val EVENT_1: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1563788371969,"mid":"LMS.1563788371969.590c5fa0-0ce8-46ed-bf6c-681c0a1fdac8","actor":{"type":"System","id":"Course Batch Updater"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}},"object":{"type":"CourseBatchEnrolment","id":"0126083288437637121_8454cb21-3ce9-4e30-85b5-fade097880d8"},"edata":{"contents":[{"contentId":"do_11260735471149056012299","status":2},{"contentId":"do_11260735471149056012300","status":1},{"contentId":"do_11260735471149056012301","status":1}],"action":"batch-enrolment-update","iteration":1,"batchId":"0126083288437637121","userId":"8454cb21-3ce9-4e30-85b5-fade097880d8","courseId":"do_1127212344324751361295"}}
      |""".stripMargin

  val EVENT_1_LEAF_NODES = Map("do_1127212344324751361295:leafnodes" -> List("do_11260735471149056012299", "do_11260735471149056012300", "do_11260735471149056012301"))


  val EVENT_2: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1563788371969,"mid":"LMS.1563788371969.590c5fa0-0ce8-46ed-bf6c-681c0a1fdac8","actor":{"type":"System","id":"Course Batch Updater"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}},"object":{"type":"CourseBatchEnrolment","id":"0126083288437637121_8454cb21-3ce9-4e30-85b5-fade097880d8"},"edata":{"contents":[{"contentId":"do_11260735471149056012299","status":2},{"contentId":"do_11260735471149056012300","status":2},{"contentId":"do_11260735471149056012301","status":1}],"action":"batch-enrolment-update","iteration":1,"batchId":"0126083288437637121","userId":"8454cb21-3ce9-4e30-85b5-fade097880d8","courseId":"do_1127212344324751361295"}}
      |""".stripMargin


  val EVENT_3: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1563788371969,"mid":"LMS.1563788371969.590c5fa0-0ce8-46ed-bf6c-681c0a1fdac8","actor":{"type":"System","id":"Course Batch Updater"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}},"object":{"type":"CourseBatchEnrolment","id":"0126083288437637121_8454cb21-3ce9-4e30-85b5-fade097880d8"},"edata":{"contents":[{"contentId":"do_11260735471149056012299","status":2},{"contentId":"do_11260735471149056012300","status":2},{"contentId":"do_11260735471149056012301","status":1}],"action":"batch-enrolment-update","iteration":1,"batchId":"0126083288437637121","userId":"8454cb21-3ce9-4e30-85b5-fade097880d8","courseId":"do_1127212344324751361295"}}
      |""".stripMargin
}