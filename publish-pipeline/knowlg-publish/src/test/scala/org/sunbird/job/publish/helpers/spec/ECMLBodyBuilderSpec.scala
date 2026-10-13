package org.sunbird.job.publish.helpers.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.knowlg.publish.helpers.ECMLBodyBuilder
import org.sunbird.job.knowlg.task.KnowlgPublishConfig

class ECMLBodyBuilderSpec extends FlatSpec with Matchers {

  implicit val formats: Formats = DefaultFormats
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)

  // Shape of an AssessmentItem created through the API: no body, question in `name`, choices in `options`.
  val apiItem: Map[String, AnyRef] = Map(
    "identifier" -> "do_ai_1", "name" -> "What is the SI unit of power?", "qlevel" -> "EASY", "type" -> "mcq",
    "options" -> List(
      Map("value" -> Int.box(0), "text" -> "Watt", "answer" -> Boolean.box(true)),
      Map("value" -> Int.box(1), "text" -> "Joule", "answer" -> Boolean.box(false))))

  "effectiveBody" should "return an item's own body unchanged when it has one (legacy question-editor items)" in {
    val body = """{"data":{"data":{"question":{"text":"<p>Q</p>"},"options":[]},"config":{}}}"""
    ECMLBodyBuilder.effectiveBody(Map("body" -> body)) shouldBe body
  }

  it should "build the legacy body shape from top-level question text + options for API-created items" in {
    val json = parse(ECMLBodyBuilder.effectiveBody(apiItem))
    (json \ "data" \ "data" \ "question" \ "text").extract[String] shouldBe "<p>What is the SI unit of power?</p>"
    val options = (json \ "data" \ "data" \ "options").extract[List[Map[String, Any]]]
    options.map(_("text")) shouldBe List("<p>Watt</p>", "<p>Joule</p>")
    options.map(_("isCorrect")) shouldBe List(true, false)
  }

  it should "read options delivered as Java collections too" in {
    val javaOptions = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
    javaOptions.add(new java.util.HashMap[String, AnyRef]() {{ put("text", "Watt"); put("isCorrect", Boolean.box(true)) }})
    val json = parse(ECMLBodyBuilder.effectiveBody(Map("name" -> "Q?", "options" -> javaOptions)))
    (json \ "data" \ "data" \ "options").extract[List[Map[String, Any]]].map(_("isCorrect")) shouldBe List(true)
  }

  it should "escape plain-text HTML characters rather than injecting markup" in {
    val json = parse(ECMLBodyBuilder.effectiveBody(apiItem + ("name" -> "Is 2 < 3 & 4 > 1?")))
    (json \ "data" \ "data" \ "question" \ "text").extract[String] shouldBe "<p>Is 2 &lt; 3 &amp; 4 &gt; 1?</p>"
  }

  it should "be empty when there is no body and no options, so the job can skip the item" in {
    ECMLBodyBuilder.effectiveBody(Map("name" -> "Orphan question")) shouldBe ""
  }

  "buildEcmlBodyFromItems" should "fill each question's data and config for API-created items instead of leaving them blank" in {
    val body = parse(ECMLBodyBuilder.buildEcmlBodyFromItems(List(apiItem), "Quiz", jobConfig))
    val questions = (body \\ "org.ekstep.question").children.headOption.getOrElse(body \\ "org.ekstep.question")
    val data = (questions \ "data" \ "__cdata").extract[String]
    val conf = (questions \ "config" \ "__cdata").extract[String]
    data should include ("What is the SI unit of power?")
    data should include ("Watt")
    conf should not be empty
  }
}
