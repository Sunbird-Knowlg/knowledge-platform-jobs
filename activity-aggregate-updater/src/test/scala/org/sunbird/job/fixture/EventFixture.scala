package org.sunbird.job.fixture

object EventFixture {

  /**
   * case-1. Inserting a first course which is having 3 leaf nodes in the redis database and cassandra database
   * does not contains this course id.
   * courseId =
   * ============BE_JOB_REQUEST_CONTENTS==========
   * do_1127212344324751361295 - course
   * do_course_unit1 - unit1
   * do_11260735471149056012299 - resource
   * do_course_unit2 - unit2
   * do_11260735471149056012300 - resource
   * do_course_unit3 - unit3
   * do_11260735471149056012301 - resource
   * do_11260735471149056012300 -resource
   *
   *
   * ============== content status in the event ======
   * do_11260735471149056012299 - 2
   * do_11260735471149056012301 - 1
   * do_11260735471149056012300 - 1
   * ============== content status in the database(content-consumption)
   * do_11260735471149056012299 - 2
   * do_11260735471149056012301 - 1
   * do_11260735471149056012300 - 2
   *
   * ============== Computation ==============
   * unit level computation
   * do_11260735471149056012299:ansestor -> do_course_unit1, do_1127212344324751361295
   * do_course_unit1:do_1127212344324751361295:leafnodes: do_11260735471149056012299
   * lefNodesSize = 1, completed = 1
   * 1/1 = 100%
   *
   * do_11260735471149056012301:ansestor -> do_course_unit3,do_1127212344324751361295
   * do_course_unit3:do_1127212344324751361295:leafNodes -> do_11260735471149056012301,do_11260735471149056012300
   * leafNodesSize = 2,completed 1
   * 1/2 = 50%
   *
   * do_11260735471149056012300:ansestor -> do_course_unit3,do_course_unit2,do_1127212344324751361295
   * do_course_unit3:do_1127212344324751361295:leafNodes -> do_11260735471149056012301,do_11260735471149056012300
   * leafNodesSize = 2, completed = 1
   * 1/2 = 50%
   *
   * do_course_unit2:do_1127212344324751361295:leafNodes -> do_11260735471149056012300
   * leafNodesSize = 1, completed = 1
   * 1/1 = 100%
   *
   * course level progress computation
   * do_1127212344324751361295:leafNodes = do_11260735471149056012299, do_11260735471149056012301, do_11260735471149056012300
   */

  val EVENT_1: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1563788371969,"mid":"LMS.1563788371969.590c5fa0-0ce8-46ed-bf6c-681c0a1fdac8","actor":{"type":"System","id":"Course Batch Updater"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}},"object":{"type":"CourseBatchEnrolment","id":"0126083288437637121_8454cb21-3ce9-4e30-85b5-fade097880d8"},"edata":{"contents":[{"contentId":"do_11260735471149056012299","status":2},{"contentId":"do_11260735471149056012300","status":1},{"contentId":"do_11260735471149056012301","status":1}],"action":"batch-enrolment-update","iteration":1,"batchId":"0126083288437637121","userId":"8454cb21-3ce9-4e30-85b5-fade097880d8","courseId":"do_1127212344324751361295"}}
      |""".stripMargin

  val courseLeafNodes = Map("do_1127212344324751361295:do_1127212344324751361295:leafnodes" -> List("do_11260735471149056012299", "do_11260735471149056012300", "do_11260735471149056012301"))
  val unitLeafNodes_1 = Map("do_1127212344324751361295:do_course_unit1:leafnodes" -> List("do_11260735471149056012299"))
  val unitLeafNodes_2 = Map("do_1127212344324751361295:do_course_unit2:leafnodes" -> List("do_11260735471149056012300"))
  val unitLeafNodes_3 = Map("do_1127212344324751361295:do_course_unit3:leafnodes" -> List("do_11260735471149056012301", "do_11260735471149056012300"))

  val ancestorsResource_1 = Map("do_1127212344324751361295:do_11260735471149056012299:ancestors" -> List("do_course_unit1", "do_1127212344324751361295"))
  val ancestorsResource_2 = Map("do_1127212344324751361295:do_11260735471149056012300:ancestors" -> List("do_course_unit2", "do_course_unit3", "do_1127212344324751361295"))
  val ancestorsResource_3 = Map("do_1127212344324751361295:do_11260735471149056012301:ancestors" -> List("do_course_unit3", "do_1127212344324751361295"))

  val CASE_1:Map[String, AnyRef] = Map("event" -> EVENT_1, "cacheData" -> List(courseLeafNodes,
    unitLeafNodes_1, unitLeafNodes_2,unitLeafNodes_3, ancestorsResource_1,ancestorsResource_2,ancestorsResource_3))



  val EVENT_2: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1563788371969,"mid":"LMS.1563788371969.590c5fa0-0ce8-46ed-bf6c-681c0a1fdac8","actor":{"type":"System","id":"Course Batch Updater"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}},"object":{"type":"CourseBatchEnrolment","id":"0126083288437637121_8454cb21-3ce9-4e30-85b5-fade097880d8"},"edata":{"contents":[{"contentId":"do_R1","status":1.1},{"contentId":"do_R2","status":1},{"contentId":"do_R3","status":1}],"action":"batch-enrolment-update","iteration":1,"batchId":"Batch1","userId":"user001","courseId":"course001"}}
      |""".stripMargin

  /** **** course structure ****
   *
   * case2: When all resource progress is 2 in the content-consumption table
   *
   * course001 -  course
   * unit1 - unit1
   * do_R1 - Resource
   * do_R3 - Resource
   * unit2 - Unit2
   * do_R2 - Resource
   * do_R3 - Resource
   *
   * ============== content status in the event ======
   * do_R1 - 1
   * do_R3 - 1
   * do_R2 - 1
   * ============== content status in the database(content-consumption)
   * do_R1 - 2
   * do_R2 - 2
   * do_R3 - 2
   *
   * //Unit Level
   * course001:do_R1:ansestor =>  unit1,course001
   * unit1:leafNodes -> do_R1,do_R3
   * output:leafNodesSize = 2, completed=2
   * course001:do_R2:ansestor =>  unit2,course001
   * unit2:leafNodes -> do_R2,do_R3
   * output:leafNodesSize = 2, completed = 2
   * course001:do_R3:ansestor =>  unit1, unit2,course001
   * unit1:leafNodes -> do_R1,do_R3
   * unit2:leafNodes -> do_R2,do_R3
   * output:leafNodes=2, completed=2
   * // CourseLevel
   * output:LeafNodes =3, Completed =3
   *
   *
   */
  val e2_courseLeafNodes = Map("course001:course001:leafnodes" -> List("do_R1", "do_R3", "do_R2"))
  val e2_unitLeafNodes_1 = Map("course001:unit1:leafnodes" -> List("do_R1", "do_R3"))
  val e2_unitLeafNodes_2 = Map("course001:unit2:leafnodes" -> List("do_R2", "do_R3"))

  val e2_ancestorsResource_1 = Map("course001:do_R1:ancestors" -> List("unit1", "course001"))
  val e2_ancestorsResource_2 = Map("course001:do_R3:ancestors" -> List("unit1", "unit2", "course001"))
  val e2_ancestorsResource_3 = Map("course001:do_R2:ancestors" -> List("unit2", "course001"))

  val CASE_2:Map[String, AnyRef] = Map("event" -> EVENT_2, "cacheData" -> List(e2_courseLeafNodes,
    e2_unitLeafNodes_1, e2_unitLeafNodes_2,e2_ancestorsResource_1, e2_ancestorsResource_2,e2_ancestorsResource_3))

  /** *
   *
   * Case3: When resource data is not available in the content_consumption table and user_activity_agg
   *
   * C11 -  course
   * unit11 - unit1
   * R11 - Resource
   * R22 - Resource
   * unit22 - Unit2
   * R11 - Resource
   *
   * ============== content status in the event ======
   * R11 - 2
   * R22 - 2
   * ============== content status in the database(content-consumption)
   * Data is not available
   *
   * //Unit Level
   * C11:R11:ansestor =>  unit11,unit22,C11
   * unit11:leafNodes -> R11,R22
   * output:leafNodesSize = 2, completed=2
   * unit11:leafNodes -> R11
   * output:leafNodesSize = 1, completed=1
   * C11:R22:ansestor =>  unit11,C11
   * unit11:leafNodes -> R11,R22
   * output:leafNodesSize = 2, completed = 2
   * // CourseLevel
   * output:LeafNodes =2, Completed =2
   *
   */

  val e3_courseLeafNodes = Map("C11:C11:leafnodes" -> List("R11", "R22"))
  val e3_unitLeafNodes_1 = Map("C11:unit11:leafnodes" -> List("R11", "R22"))
  val e3_unitLeafNodes_2 = Map("C11:unit22:leafnodes" -> List("R11"))

  val e3_ancestorsResource_1 = Map("C11:R11:ancestors" -> List("unit11", "C11"))
  val e3_ancestorsResource_2 = Map("C11:R11:ancestors" -> List("unit22", "C11"))
  val e3_ancestorsResource_3 = Map("C11:R22:ancestors" ->  List("unit11", "C11"))

  val EVENT_3: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1563788371969,"mid":"LMS.1563788371969.590c5fa0-0ce8-46ed-bf6c-681c0a1fdac8","actor":{"type":"System","id":"Course Batch Updater"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}},"object":{"type":"CourseBatchEnrolment","id":"0126083288437637121_8454cb21-3ce9-4e30-85b5-fade097880d8"},"edata":{"contents":[{"contentId":"R11","status":2},{"contentId":"R22","status":2}],"action":"batch-enrolment-update","iteration":1,"batchId":"B11","userId":"U11","courseId":"C11"}}
      |""".stripMargin

  val CASE_3:Map[String, AnyRef] = Map("event" -> EVENT_3, "cacheData" -> List(e3_courseLeafNodes, e3_unitLeafNodes_1, e3_unitLeafNodes_2,e3_ancestorsResource_1, e3_ancestorsResource_2, e3_ancestorsResource_3) )

  val EVENT_4: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1563788371969,"mid":"LMS.1563788371969.590c5fa0-0ce8-46ed-bf6c-681c0a1fdac8","actor":{"type":"System","id":"Course Batch Updater"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}},"object":{"type":"CourseBatchEnrolment","id":"0126083288437637121_8454cb21-3ce9-4e30-85b5-fade097880d8"},"edata":{"contents":[{"contentId":"do_11260735471149056012299","status":2},{"contentId":"do_11260735471149056012300","status":1},{"contentId":"do_11260735471149056012301","status":1}],"action":"batch-enrolment-update","iteration":1,"batchId":"0126083288437637121","userId":"8454cb21-3ce9-4e30-85b5-fade097880d8","courseId":"do_1127212344324751361295"}}
      |""".stripMargin

  val EVENT_5: String =
    """
      |{"eid":"BE_JOB_REQUEST","ets":1563788371969,"mid":"LMS.1563788371969.590c5fa0-0ce8-46ed-bf6c-681c0a1fdac8","actor":{"type":"System","id":"Course Batch Updater"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"}},"object":{"type":"CourseBatchEnrolment","id":"0126083288437637121_8454cb21-3ce9-4e30-85b5-fade097880d8"},"edata":{"contents":[{"contentId":"do_11260735471149056012299","status":2},{"contentId":"do_11260735471149056012300","status":1},{"contentId":"do_11260735471149056012301","status":1}],"action":"batch-update","iteration":1,"batchId":"0126083288437637121","userId":"8454cb21-3ce9-4e30-85b5-fade097880d8","courseId":"do_1127212344324751361295"}}
      |""".stripMargin
}