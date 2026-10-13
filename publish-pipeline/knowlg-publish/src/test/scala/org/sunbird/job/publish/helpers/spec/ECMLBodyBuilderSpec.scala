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

  it should "not double-escape text that already carries HTML entities" in {
    val json = parse(ECMLBodyBuilder.effectiveBody(apiItem + ("name" -> "Newton&#39;s law & F&#61;ma")))
    (json \ "data" \ "data" \ "question" \ "text").extract[String] shouldBe "<p>Newton&#39;s law &amp; F&#61;ma</p>"
  }

  def unitBody(pluginId: String, ver: String, template: String, data: String): String =
    s"""{"data":{"plugin":{"id":"$pluginId","version":"$ver","templateId":"$template"},"data":$data,"config":{"metadata":{"qlevel":"EASY","category":"X"},"max_score":1}}}"""

  // Body as written by the legacy ECML editor's FTB question unit (from a real AssessmentItem), keyboard media included.
  val ftbMedia = """[{"id":"org.ekstep.keyboard.eras_icon","src":"/content-plugins/org.ekstep.keyboard-1.1/renderer/assets/eras_icon.png","assetId":"org.ekstep.keyboard.eras_icon","type":"image","preload":true},{"id":"org.ekstep.keyboard.hide_keyboard","src":"/content-plugins/org.ekstep.keyboard-1.1/renderer/assets/keyboard.svg","assetId":"org.ekstep.keyboard.hide_keyboard","type":"image","preload":true}]"""
  val ftbItem: Map[String, AnyRef] = Map("identifier" -> "do_ftb_1", "name" -> "The grass is ____", "qlevel" -> "EASY", "type" -> "ftb",
    "body" -> s"""{"data":{"plugin":{"id":"org.ekstep.questionunit.ftb","version":"1.1","templateId":"ftbtemplate"},"data":{"question":{"text":"<p>The grass is [[green]]</p>","image":"","audio":"","audioName":"","keyboardConfig":{"keyboardType":"Device","customKeys":[]}},"answer":["green"],"media":$ftbMedia},"config":{"metadata":{"max_score":1,"qlevel":"EASY","category":"FTB"},"max_score":1,"questionCount":1},"media":$ftbMedia}}""")
  // Real legacy-editor Sequence item: top-level type/category say "mcq", only the body plugin identifies it.
  val seqItem: Map[String, AnyRef] = Map("identifier" -> "do_seq_1", "name" -> "Arrange the following", "qlevel" -> "EASY", "type" -> "mcq", "category" -> "mcq",
    "body" -> """{"data":{"plugin":{"id":"org.ekstep.questionunit.sequence","version":"1.1","templateId":"sequenceTemplate"},"data":{"question":{"text":"<p>Arrange the following</p>","image":"","audio":"","audioName":"","hint":""},"options":[{"text":"1234","image":"","audio":"","audioName":"","hint":""},{"text":"5678","image":"","audio":"","audioName":"","hint":""}],"media":[]},"config":{"metadata":{"max_score":1,"qlevel":"EASY","category":"mcq"},"max_score":1,"questionCount":1},"media":[]}}""")
  // Real legacy-editor MTF and Reorder bodies (MTF data uses `option.optionsLHS/RHS`; Reorder uses `sentence.tabs` and has no media block).
  val mtfItem: Map[String, AnyRef] = Map("identifier" -> "do_mtf_1", "name" -> "Match the month with the holiday", "qlevel" -> "EASY", "type" -> "mtf",
    "body" -> """{"data":{"plugin":{"id":"org.ekstep.questionunit.mtf","version":"1.2","templateId":"horizontalMTF"},"data":{"question":{"text":"<p>Match the month with the holiday</p>","image":"","audio":"","audioName":"","hint":""},"option":{"optionsLHS":[{"text":"<p>Christmas</p>","index":1},{"text":"<p>New Year</p>","index":2}],"optionsRHS":[{"text":"<p>December</p>","mapIndex":1},{"text":"<p>January</p>","mapIndex":2}],"questionCount":0},"media":[]},"config":{"metadata":{"max_score":1,"qlevel":"EASY","category":"MTF"},"max_score":1,"questionCount":1},"media":[]}}""")
  val reoItem: Map[String, AnyRef] = Map("identifier" -> "do_reo_1", "name" -> "Arrange the given words", "qlevel" -> "EASY", "type" -> "mcq", "category" -> "mcq",
    "body" -> """{"data":{"plugin":{"id":"org.ekstep.questionunit.reorder","version":"1.1","templateId":"reorderingtemplate"},"data":{"question":{"text":"<p>Arrange the given words in proper order to form a sentence.</p>"},"sentence":{"text":"The grass is green","tabs":[{"text":"The","id":0},{"text":"grass","id":1},{"text":"is","id":2},{"text":"green","id":3}]}},"config":{"metadata":{"max_score":1,"qlevel":"EASY","category":"mcq"},"max_score":1,"questionCount":1}}}""")

  def rootQuestions(body: JValue): List[JValue] = (body \\ "org.ekstep.question") match {
    case JArray(qs) => qs
    case q          => List(q)
  }
  def quizConfig(body: JValue): JValue = {
    val qs = (body \\ "org.ekstep.questionset") match { case JArray(List(o)) => o; case o => o }
    parse((qs \ "config" \ "__cdata").extract[String])
  }
  def pluginIds(body: JValue): List[String] = (body \ "theme" \ "plugin-manifest" \ "plugin" \ "id").extract[List[String]]
  def mediaSrcs(body: JValue): List[String] = (body \ "theme" \ "manifest" \ "media" \ "src").extract[List[String]]

  "isRenderable" should "accept supported question units and MCQ-shaped bodies, and reject unknown plugins or empty items" in {
    ECMLBodyBuilder.isRenderable(apiItem) shouldBe true
    ECMLBodyBuilder.isRenderable(ftbItem) shouldBe true
    ECMLBodyBuilder.isRenderable(Map("body" -> unitBody("org.ekstep.questionunit.unknown", "1.0", "t", "{}"))) shouldBe false
    ECMLBodyBuilder.isRenderable(Map("name" -> "Orphan question")) shouldBe false
    ECMLBodyBuilder.isRenderable(Map("body" -> """{"data":{"plugin":{"id":"org.ekstep.questionunit.ftb","version":"1.1"},"config":{}}}""")) shouldBe false
  }

  it should "not repeat a plugin asset that an item also lists in its own media" in {
    val dup = """[{"id":"org.ekstep.questionunit.mtf.shape1.png","src":"/content-plugins/org.ekstep.questionunit.mtf-1.2/renderer/assets/shape1.png","type":"image"}]"""
    val item = mtfItem.updated("body", mtfItem("body").toString.replace(""""media":[]}""", s""""media":$dup}"""))
    val body = parse(ECMLBodyBuilder.buildEcmlBodyFromItems(List(item), "Quiz", jobConfig))
    mediaSrcs(body).count(_ == "/content-plugins/org.ekstep.questionunit.mtf-1.2/renderer/assets/shape1.png") shouldBe 1
  }

  it should "list the keyboard plugin before the FTB unit that depends on it" in {
    val ids = pluginIds(parse(ECMLBodyBuilder.buildEcmlBodyFromItems(List(ftbItem), "Quiz", jobConfig)))
    ids.indexOf("org.ekstep.keyboard") should be < ids.indexOf("org.ekstep.questionunit.ftb")
  }

  "buildEcmlBodyFromItems" should "keep the MCQ-only output on the MCQ 1.3 plugin and bundle no other question units" in {
    val body = parse(ECMLBodyBuilder.buildEcmlBodyFromItems(List(apiItem), "Quiz", jobConfig))
    val q = rootQuestions(body).head
    (q \ "pluginId").extract[String] shouldBe "org.ekstep.questionunit.mcq"
    (q \ "pluginVer").extract[String] shouldBe "1.3"
    (q \ "templateId").extract[String] shouldBe "horizontalMCQ"
    (q \ "type").extract[String] shouldBe "mcq"
    pluginIds(body) should contain ("org.ekstep.questionunit.mcq")
    pluginIds(body).filter(_.startsWith("org.ekstep.questionunit.")) shouldBe List("org.ekstep.questionunit.mcq")
    mediaSrcs(body) should contain ("/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/template/mcq-layouts.js")
  }

  it should "produce exactly the previous MCQ-only manifest (same media sources in the same order, same plugin list)" in {
    val body = parse(ECMLBodyBuilder.buildEcmlBodyFromItems(List(apiItem), "Quiz", jobConfig))
    mediaSrcs(body) shouldBe List(
      "/content-plugins/org.ekstep.navigation-1.0/renderer/controller/navigation_ctrl.js",
      "/content-plugins/org.ekstep.navigation-1.0/renderer/templates/navigation.html",
      "/content-plugins/org.ekstep.navigation-1.0/renderer/plugin.js",
      "/content-plugins/org.ekstep.navigation-1.0/manifest.json",
      "/content-plugins/org.ekstep.questionset.quiz-1.0/renderer/plugin.js",
      "/content-plugins/org.ekstep.questionset.quiz-1.0/manifest.json",
      "/content-plugins/org.ekstep.iterator-1.0/renderer/plugin.js",
      "/content-plugins/org.ekstep.iterator-1.0/manifest.json",
      "/content-plugins/org.ekstep.questionset-1.0/renderer/utils/telemetry_logger.js",
      "/content-plugins/org.ekstep.questionset-1.0/renderer/utils/html_audio_plugin.js",
      "/content-plugins/org.ekstep.questionset-1.0/renderer/utils/qs_feedback_popup.js",
      "/content-plugins/org.ekstep.questionset-1.0/renderer/plugin.js",
      "/content-plugins/org.ekstep.questionset-1.0/manifest.json",
      "/content-plugins/org.ekstep.questionunit-1.2/renderer/assets/audio-icon.png",
      "/content-plugins/org.ekstep.questionunit-1.2/renderer/assets/down_arrow.png",
      "/content-plugins/org.ekstep.questionunit-1.2/renderer/assets/zoom.png",
      "/content-plugins/org.ekstep.questionunit-1.2/renderer/assets/audio-icon1.png",
      "/content-plugins/org.ekstep.questionunit-1.2/renderer/components/js/components.js",
      "/content-plugins/org.ekstep.questionunit-1.2/renderer/components/css/components.css",
      "/content-plugins/org.ekstep.questionunit-1.2/renderer/plugin.js",
      "/content-plugins/org.ekstep.questionunit-1.2/manifest.json",
      "/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/styles/style.css",
      "/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/styles/horizontal_and_vertical.css",
      "/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/template/mcq-layouts.js",
      "/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/template/template_controller.js",
      "/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/template/question-component.js",
      "/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/assets/tick_icon.png",
      "/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/assets/audio-icon2.png",
      "/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/assets/music-blue.png",
      "/content-plugins/org.ekstep.questionunit.mcq-1.3/renderer/plugin.js",
      "/content-plugins/org.ekstep.questionunit.mcq-1.3/manifest.json",
      "/content-plugins/org.ekstep.summary-1.0/renderer/summary-template.js",
      "/content-plugins/org.ekstep.summary-1.0/renderer/style.css",
      "/content-plugins/org.ekstep.summary-1.0/renderer/plugin.js",
      "/content-plugins/org.ekstep.summary-1.0/manifest.json",
      "/content-plugins/org.ekstep.summary-1.0/assets/summary-icon.jpg",
      "/content-plugins/org.ekstep.questionset-1.0/editor/assets/quizimage.png")
    pluginIds(body) shouldBe List("org.ekstep.navigation", "org.ekstep.questionset.quiz", "org.ekstep.iterator", "org.ekstep.questionset",
      "org.ekstep.questionunit", "org.ekstep.questionunit.mcq", "org.ekstep.summary")
  }

  it should "record the target and each question's level in the quiz config so the player can draw a per-learner subset" in {
    val items = List(apiItem.updated("identifier", "do_e1"), apiItem.updated("identifier", "do_m1").updated("qlevel", "medium"))
    val body = parse(ECMLBodyBuilder.buildEcmlBodyFromItems(items, "Quiz", jobConfig, Map("E" -> 1, "M" -> 1, "D" -> 0)))
    val qsConfig = quizConfig(body)
    (qsConfig \ "dynamic_assess" \ "target").extract[Map[String, Int]] shouldBe Map("E" -> 1, "M" -> 1, "D" -> 0)
    (qsConfig \ "dynamic_assess" \ "levels").extract[Map[String, String]] shouldBe Map("do_e1" -> "EASY", "do_m1" -> "MEDIUM")
    (qsConfig \ "total_items").extract[Int] shouldBe 2
  }

  it should "leave the quiz config untouched when no target is given" in {
    val body = parse(ECMLBodyBuilder.buildEcmlBodyFromItems(List(apiItem), "Quiz", jobConfig))
    val qsConfig = quizConfig(body)
    (qsConfig \ "dynamic_assess") shouldBe JNothing
  }

  it should "render an FTB item with its own plugin, pass its data through, and bundle the keyboard plugin it needs" in {
    val body = parse(ECMLBodyBuilder.buildEcmlBodyFromItems(List(ftbItem), "Quiz", jobConfig))
    val q = rootQuestions(body).head
    (q \ "pluginId").extract[String] shouldBe "org.ekstep.questionunit.ftb"
    (q \ "pluginVer").extract[String] shouldBe "1.1"
    (q \ "templateId").extract[String] shouldBe "ftbtemplate"
    (q \ "type").extract[String] shouldBe "ftb"
    (parse((q \ "data" \ "__cdata").extract[String]) \ "answer").extract[List[String]] shouldBe List("green")
    pluginIds(body) should contain allOf ("org.ekstep.questionunit.ftb", "org.ekstep.keyboard")
    pluginIds(body) should not contain "org.ekstep.questionunit.mcq"
    mediaSrcs(body) should contain allOf ("/content-plugins/org.ekstep.questionunit.ftb-1.1/renderer/ftbcontroller.js", "/content-plugins/org.ekstep.keyboard-1.1/renderer/keyboard.js")
  }

  it should "add each item's own media to the content manifest once, even when listed in both media blocks" in {
    val body = parse(ECMLBodyBuilder.buildEcmlBodyFromItems(List(ftbItem, ftbItem.updated("identifier", "do_ftb_2")), "Quiz", jobConfig))
    mediaSrcs(body).count(_ == "/content-plugins/org.ekstep.keyboard-1.1/renderer/assets/eras_icon.png") shouldBe 1
    mediaSrcs(body) should contain ("/content-plugins/org.ekstep.keyboard-1.1/renderer/assets/keyboard.svg")
  }

  it should "render a Sequence item with the sequence plugin even though its top-level type says mcq" in {
    val body = parse(ECMLBodyBuilder.buildEcmlBodyFromItems(List(seqItem), "Quiz", jobConfig))
    val q = rootQuestions(body).head
    (q \ "pluginId").extract[String] shouldBe "org.ekstep.questionunit.sequence"
    (q \ "templateId").extract[String] shouldBe "sequenceTemplate"
    (q \ "type").extract[String] shouldBe "sequence"
    (parse((q \ "data" \ "__cdata").extract[String]) \ "options" \ "text").extract[List[String]] shouldBe List("1234", "5678")
    pluginIds(body) should contain ("org.ekstep.questionunit.sequence")
    pluginIds(body) should not contain "org.ekstep.questionunit.mcq"
  }

  it should "package all five legacy question units from real editor bodies in one quiz, each with its own template" in {
    val body = parse(ECMLBodyBuilder.buildEcmlBodyFromItems(List(apiItem, ftbItem, mtfItem, reoItem, seqItem), "Quiz", jobConfig))
    rootQuestions(body).map(q => ((q \ "pluginId").extract[String], (q \ "templateId").extract[String], (q \ "type").extract[String])) shouldBe List(
      ("org.ekstep.questionunit.mcq", "horizontalMCQ", "mcq"),
      ("org.ekstep.questionunit.ftb", "ftbtemplate", "ftb"),
      ("org.ekstep.questionunit.mtf", "horizontalMTF", "mtf"),
      ("org.ekstep.questionunit.reorder", "reorderingtemplate", "reorder"),
      ("org.ekstep.questionunit.sequence", "sequenceTemplate", "sequence"))
    val reoData = parse((rootQuestions(body)(3) \ "data" \ "__cdata").extract[String])
    (reoData \ "sentence" \ "tabs" \ "text").extract[List[String]] shouldBe List("The", "grass", "is", "green")
    pluginIds(body).filter(_.startsWith("org.ekstep.questionunit.")) shouldBe List("org.ekstep.questionunit.mcq", "org.ekstep.questionunit.ftb", "org.ekstep.questionunit.mtf", "org.ekstep.questionunit.reorder", "org.ekstep.questionunit.sequence")
  }

  it should "bundle each question unit once when a quiz mixes types" in {
    val body = parse(ECMLBodyBuilder.buildEcmlBodyFromItems(List(apiItem, mtfItem, apiItem.updated("identifier", "do_ai_2")), "Quiz", jobConfig))
    rootQuestions(body).map(q => (q \ "pluginId").extract[String]) shouldBe List("org.ekstep.questionunit.mcq", "org.ekstep.questionunit.mtf", "org.ekstep.questionunit.mcq")
    pluginIds(body).filter(_.startsWith("org.ekstep.questionunit.")) shouldBe List("org.ekstep.questionunit.mcq", "org.ekstep.questionunit.mtf")
    mediaSrcs(body).count(_ == "/content-plugins/org.ekstep.questionunit.mtf-1.2/renderer/plugin.js") shouldBe 1
  }

  it should "fill each question's data and config for API-created items instead of leaving them blank" in {
    val body = parse(ECMLBodyBuilder.buildEcmlBodyFromItems(List(apiItem), "Quiz", jobConfig))
    val questions = (body \\ "org.ekstep.question").children.headOption.getOrElse(body \\ "org.ekstep.question")
    val data = (questions \ "data" \ "__cdata").extract[String]
    val conf = (questions \ "config" \ "__cdata").extract[String]
    data should include ("What is the SI unit of power?")
    data should include ("Watt")
    conf should not be empty
  }
}
