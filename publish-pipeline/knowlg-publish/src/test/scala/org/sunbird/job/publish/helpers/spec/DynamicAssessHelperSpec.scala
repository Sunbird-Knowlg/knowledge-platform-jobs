package org.sunbird.job.publish.helpers.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.knowlg.publish.helpers.DynamicAssessHelper
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.when
import org.sunbird.job.util.{HTTPResponse, HttpUtil, ScalaJsonUtil}

class DynamicAssessHelperSpec extends FlatSpec with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)
  val helper = new DynamicAssessHelper(jobConfig, mock[HttpUtil])

  "updateGeneratedContent" should "save the body and learner count through the content update API with the read's versionKey" in {
    val http = mock[HttpUtil]
    val body = org.mockito.ArgumentCaptor.forClass(classOf[String])
    when(http.patch(org.mockito.ArgumentMatchers.endsWith("/content/v3/update/do_c"), body.capture(), org.mockito.ArgumentMatchers.any())).thenReturn(HTTPResponse(200, """{"responseCode":"OK"}"""))
    new DynamicAssessHelper(jobConfig, http).updateGeneratedContent("do_c", "{\"theme\":{}}", 6, "vk1") shouldBe true
    val sent = ScalaJsonUtil.deserialize[Map[String, AnyRef]](body.getValue)
    val content = sent("request").asInstanceOf[Map[String, AnyRef]]("content").asInstanceOf[Map[String, AnyRef]]
    content("body") shouldBe "{\"theme\":{}}"
    content("totalQuestions").toString shouldBe "6"
    content("versionKey") shouldBe "vk1"
    content.keySet shouldBe Set("body", "totalQuestions", "versionKey")
  }

  it should "retry once with the newer versionKey when the content was saved after the job read it" in {
    val http = mock[HttpUtil]
    val update = org.mockito.ArgumentMatchers.endsWith("/content/v3/update/do_c")
    when(http.patch(update, org.mockito.ArgumentMatchers.contains("\"vk1\""), org.mockito.ArgumentMatchers.any())).thenReturn(HTTPResponse(400, """{"params":{"errmsg":"Invalid version Key"}}"""))
    when(http.patch(org.mockito.ArgumentMatchers.endsWith("/content/v3/update/do_c"), org.mockito.ArgumentMatchers.contains("\"vk2\""), org.mockito.ArgumentMatchers.any())).thenReturn(HTTPResponse(200, """{"responseCode":"OK"}"""))
    when(http.get(org.mockito.ArgumentMatchers.endsWith("/content/v3/read/do_c?mode=edit"), org.mockito.ArgumentMatchers.any()))
      .thenReturn(HTTPResponse(200, """{"result":{"content":{"versionKey":"vk2"}}}"""))
    new DynamicAssessHelper(jobConfig, http).updateGeneratedContent("do_c", "{}", 6, "vk1") shouldBe true
  }

  it should "not resend the same request when the failure isn't a newer versionKey (negative)" in {
    val http = mock[HttpUtil]
    when(http.patch(anyString(), anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(HTTPResponse(500, "{}"))
    when(http.get(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(HTTPResponse(200, """{"result":{"content":{"versionKey":"vk1"}}}"""))
    new DynamicAssessHelper(jobConfig, http).updateGeneratedContent("do_c", "{}", 6, "vk1") shouldBe false
    org.mockito.Mockito.verify(http, org.mockito.Mockito.times(1)).patch(anyString(), anyString(), org.mockito.ArgumentMatchers.any())
  }

  it should "report failure instead of throwing when the call errors (negative)" in {
    val http = mock[HttpUtil]
    when(http.patch(anyString(), anyString(), org.mockito.ArgumentMatchers.any())).thenThrow(new RuntimeException("connection refused"))
    new DynamicAssessHelper(jobConfig, http).updateGeneratedContent("do_c", "{}", 6, "vk1") shouldBe false
  }

  "learnerPoolSize" should "publish required x multiplier questions per bucket so each learner can draw their own set" in {
    helper.learnerPoolSize(4, 3) shouldBe 12
    helper.learnerPoolSize(4, 5) shouldBe 20
    helper.learnerPoolSize(0, 3) shouldBe 0
  }

  "nodeSetting" should "use the content's own minCriteria/multiplier in whatever shape the read returned" in {
    val props = new java.util.HashMap[String, AnyRef]()
    props.put("minCriteria", Integer.valueOf(4))
    props.put("multiplier", "5")
    helper.nodeSetting(props, "minCriteria", 0, 2) shouldBe 4
    helper.nodeSetting(props, "multiplier", 1, 3) shouldBe 5
    props.put("minCriteria", java.lang.Double.valueOf(0.0))
    helper.nodeSetting(props, "minCriteria", 0, 2) shouldBe 0
  }

  it should "fall back to config when the content has no value, or one that is unusable (negative)" in {
    val props = new java.util.HashMap[String, AnyRef]()
    helper.nodeSetting(props, "minCriteria", 0, 2) shouldBe 2
    props.put("multiplier", Integer.valueOf(0))
    helper.nodeSetting(props, "multiplier", 1, 3) shouldBe 3
    props.put("multiplier", "abc")
    helper.nodeSetting(props, "multiplier", 1, 3) shouldBe 3
    props.put("minCriteria", Integer.valueOf(-1))
    helper.nodeSetting(props, "minCriteria", 0, 2) shouldBe 2
  }

  "learnerQuestionCount" should "count what one learner sees: the target per level, capped by the pool at that level" in {
    helper.learnerQuestionCount(Map("E" -> 2, "M" -> 1, "D" -> 0), List.fill(6)("EASY") ++ List.fill(3)("MEDIUM")) shouldBe 3
    helper.learnerQuestionCount(Map("E" -> 2, "M" -> 1, "D" -> 1), List("EASY", "medium")) shouldBe 2
  }

  "effectiveAvailability" should "let a bucket supply only available / multiplier questions" in {
    helper.effectiveAvailability(10, 3) shouldBe 3
    helper.effectiveAvailability(12, 3) shouldBe 4
    helper.effectiveAvailability(5, 3) shouldBe 1
    helper.effectiveAvailability(2, 3) shouldBe 0
  }

  "allocate with multiplier-scaled availability" should "cover a target only when every level has target x multiplier questions (positive)" in {
    val pool = Map(("Physics", "E") -> 12, ("Physics", "M") -> 12, ("Physics", "D") -> 12)
    val target = Map("E" -> 4, "M" -> 4, "D" -> 4)
    val result = helper.allocate(List("Physics"), target, 2, (s, l) => helper.effectiveAvailability(pool.getOrElse((s, l), 0), 3))
    helper.uncoveredLevels(result.allocations, target) shouldBe empty
  }

  it should "report the levels it can't cover when the pool is below target x multiplier (negative)" in {
    val pool = Map(("Scrum", "E") -> 5)
    val target = Map("E" -> 5, "M" -> 0, "D" -> 0)
    val result = helper.allocate(List("Scrum"), target, 2, (s, l) => helper.effectiveAvailability(pool.getOrElse((s, l), 0), 3))
    helper.uncoveredLevels(result.allocations, target) shouldBe Map("E" -> (1, 5))
  }

  it should "combine skills so their pools together cover a level" in {
    val pool = Map(("Physics", "E") -> 6, ("Chemistry", "E") -> 6)
    val target = Map("E" -> 4, "M" -> 0, "D" -> 0)
    val result = helper.allocate(List("Physics", "Chemistry"), target, 1, (s, l) => helper.effectiveAvailability(pool.getOrElse((s, l), 0), 3))
    helper.uncoveredLevels(result.allocations, target) shouldBe empty
    result.allocations.map(_.requiredCount).sum shouldBe 4
  }

  "readCurrentChildren" should "return the QuestionSet's children as stored now, so stale ones from an overlapping run are removed too" in {
    val http = mock[HttpUtil]
    when(http.get(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(HTTPResponse(200,
      """{"result":{"questionset":{"identifier":"do_qs","children":[{"identifier":"do_q1"},{"identifier":"do_q2"}]}}}"""))
    new DynamicAssessHelper(jobConfig, http).readCurrentChildren("do_qs") shouldBe Some(List("do_q1", "do_q2"))
  }

  it should "return None when the read fails, so the caller falls back to the start-of-job children" in {
    val http = mock[HttpUtil]
    when(http.get(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(HTTPResponse(500, "error"))
    new DynamicAssessHelper(jobConfig, http).readCurrentChildren("do_qs") shouldBe None
  }

  "readCurrentObject" should "read a Dynamic Course Assessment's full metadata in edit mode, so the framework's skill category field comes back whatever it is called" in {
    val http = mock[HttpUtil]
    when(http.get(org.mockito.ArgumentMatchers.endsWith("/content/v3/read/do_c?mode=edit"), org.mockito.ArgumentMatchers.any()))
      .thenReturn(HTTPResponse(200, """{"result":{"content":{"framework":"bmgs","topic":["Algebra"],"difficultyTarget":"{\"E\":2,\"M\":1,\"D\":2}"}}}"""))
    val current = new DynamicAssessHelper(jobConfig, http).readCurrentObject("Content", "do_c")
    current.map(_("topic")) shouldBe Some(List("Algebra"))
  }

  "withCurrentSettings" should "replace stale graph settings, including the skills' category field, with the fresh copy in the shapes the parsers read" in {
    val graph = new java.util.HashMap[String, AnyRef]()
    graph.put("topic", java.util.Arrays.asList("Algebra"))
    graph.put("difficultyTarget", "{\"E\":2,\"M\":2,\"D\":2}")
    graph.put("name", "Old name")
    val fresh: Map[String, AnyRef] = Map("topic" -> List("Algebra", "Geometry"), "difficultyTarget" -> Map("E" -> 2, "M" -> 1, "D" -> 2))
    val merged = helper.withCurrentSettings(graph, fresh, List("topic"))
    helper.parseSkills(merged, "topic") shouldBe List("Algebra", "Geometry")
    helper.parseDifficultyTarget(merged) shouldBe Map("E" -> 2, "M" -> 1, "D" -> 2)
    merged.get("name") shouldBe "Old name"
  }

  it should "take minCriteria and multiplier from the fresh copy, as JSON numbers come back from the read" in {
    val graph = new java.util.HashMap[String, AnyRef]()
    graph.put("minCriteria", Integer.valueOf(2))
    val fresh: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]]("""{"minCriteria":4,"multiplier":5}""")
    val merged = helper.withCurrentSettings(graph, fresh)
    helper.nodeSetting(merged, "minCriteria", 0, 2) shouldBe 4
    helper.nodeSetting(merged, "multiplier", 1, 3) shouldBe 5
  }

  it should "leave the skills' field at its graph value when it isn't passed as the category field" in {
    val graph = new java.util.HashMap[String, AnyRef]()
    graph.put("topic", java.util.Arrays.asList("Algebra"))
    val merged = helper.withCurrentSettings(graph, Map("topic" -> List("Geometry")))
    helper.parseSkills(merged, "topic") shouldBe List("Algebra")
  }

  "parseSkills" should "read the selected skills from the framework's category field, not a fixed 'skill' field" in {
    val props = new java.util.HashMap[String, AnyRef]()
    props.put("topic", java.util.Arrays.asList("Algebra", "Algebra", "Geometry"))
    props.put("skill", java.util.Arrays.asList("Ignored"))
    helper.parseSkills(props, "topic") shouldBe List("Algebra", "Geometry")
    helper.parseSkills(props, "skill") shouldBe List("Ignored")
  }

  it should "return no skills when the category field is absent (negative)" in {
    helper.parseSkills(new java.util.HashMap[String, AnyRef](), "topic") shouldBe Nil
  }

  it should "accept a difficultyTarget the API returns as a JSON string" in {
    val merged = helper.withCurrentSettings(new java.util.HashMap[String, AnyRef](), Map("difficultyTarget" -> "{\"E\":1,\"M\":0,\"D\":3}"))
    helper.parseDifficultyTarget(merged) shouldBe Map("E" -> 1, "M" -> 0, "D" -> 3)
  }

  "childIds" should "list the hierarchy's direct children" in {
    helper.childIds(Map("children" -> List(Map("identifier" -> "do_q1"), Map("identifier" -> "do_q2")))) shouldBe List("do_q1", "do_q2")
    helper.childIds(Map.empty) shouldBe Nil
  }
}
