package org.sunbird.job.publish.helpers.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.knowlg.publish.helpers.DynamicAssessHelper
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.when
import org.sunbird.job.util.{HTTPResponse, HttpUtil}

class DynamicAssessHelperSpec extends FlatSpec with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)
  val helper = new DynamicAssessHelper(jobConfig, mock[HttpUtil])

  "generatedMetadataNodeId" should "target the node itself for a never-published Draft" in {
    helper.generatedMetadataNodeId("do_1", "do_1", editCopyExists = false) shouldBe Some("do_1")
  }

  it should "target the edit copy when published content has one" in {
    helper.generatedMetadataNodeId("do_1.img", "do_1", editCopyExists = true) shouldBe Some("do_1.img")
  }

  it should "skip the write when published content has no edit copy, instead of failing on a missing node or touching live" in {
    helper.generatedMetadataNodeId("do_1.img", "do_1", editCopyExists = false) shouldBe None
  }

  "generatedContentMetadata" should "carry the question count the editor reads to know questions exist" in {
    helper.generatedContentMetadata(5)("totalQuestions") shouldBe Int.box(5)
  }

  it should "stamp lastUpdatedOn in the platform's timestamp format so a same-count regenerate is still detectable" in {
    val stamp = helper.generatedContentMetadata(5, new java.util.Date(0L))("lastUpdatedOn").toString
    stamp should fullyMatch regex """\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{4}"""
  }

  "learnerPoolSize" should "publish required x multiplier questions per bucket so each learner can draw their own set" in {
    helper.learnerPoolSize(4) shouldBe 12
    helper.learnerPoolSize(0) shouldBe 0
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

  "readCurrentObject" should "read a Dynamic Course Assessment's settings through the content read API in edit mode" in {
    val http = mock[HttpUtil]
    when(http.get(org.mockito.ArgumentMatchers.contains("/content/v3/read/do_c?mode=edit&fields=skill,difficultyTarget,framework,channel,name"), org.mockito.ArgumentMatchers.any()))
      .thenReturn(HTTPResponse(200, """{"result":{"content":{"skill":["Physics"],"difficultyTarget":"{\"E\":2,\"M\":1,\"D\":2}"}}}"""))
    val current = new DynamicAssessHelper(jobConfig, http).readCurrentObject("Content", "do_c")
    current.map(_("skill")) shouldBe Some(List("Physics"))
  }

  "withCurrentSettings" should "replace stale graph settings with the fresh copy in the shapes the parsers read" in {
    val graph = new java.util.HashMap[String, AnyRef]()
    graph.put("skill", java.util.Arrays.asList("Physics"))
    graph.put("difficultyTarget", "{\"E\":2,\"M\":2,\"D\":2}")
    graph.put("name", "Old name")
    val fresh: Map[String, AnyRef] = Map("skill" -> List("Physics", "Chemistry"), "difficultyTarget" -> Map("E" -> 2, "M" -> 1, "D" -> 2))
    val merged = helper.withCurrentSettings(graph, fresh)
    helper.parseSkills(merged) shouldBe List("Physics", "Chemistry")
    helper.parseDifficultyTarget(merged) shouldBe Map("E" -> 2, "M" -> 1, "D" -> 2)
    merged.get("name") shouldBe "Old name"
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
