package org.sunbird.job.publish.handler

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class QuestionHandlerFactorySpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  // ─── FTBHandler — happy paths ─────────────────────────────────────────────────

  "FTBHandler" should "return body as the question" in {
    val handler = QuestionHandlerFactory(Some("FTB Question"))
    handler shouldBe defined
    handler.get.getQuestion(Some(Map("body" -> "<p>Fill in the blank</p>"))) shouldBe "<p>Fill in the blank</p>"
  }

  it should "extract all mapping values across multiple blanks" in {
    val rd =
      """{
        |  "response1": {
        |    "mapping": [
        |      {"response": "response1", "value": "sun"},
        |      {"response": "response1", "value": "solar"}
        |    ]
        |  },
        |  "response2": {
        |    "mapping": [
        |      {"response": "response2", "value": "moon"}
        |    ]
        |  }
        |}""".stripMargin
    val handler = QuestionHandlerFactory(Some("FTB Question")).get
    val answers = handler.getAnswers(Some(Map("responseDeclaration" -> rd)))
    answers should contain allOf ("sun", "solar", "moon")
    answers should have size 3
  }

  it should "support multiple accepted values per blank (i18n / alternate answers)" in {
    val rd =
      """{
        |  "response1": {
        |    "mapping": [
        |      {"response": "response1", "value": "colour"},
        |      {"response": "response1", "value": "color"}
        |    ]
        |  }
        |}""".stripMargin
    val handler = QuestionHandlerFactory(Some("FTB Question")).get
    val answers = handler.getAnswers(Some(Map("responseDeclaration" -> rd)))
    answers should contain allOf ("colour", "color")
    answers should have size 2
  }

  // ─── FTBHandler — edge cases ──────────────────────────────────────────────────

  it should "return empty list when mapping array is empty" in {
    val rd = """{"response1": {"mapping": []}}"""
    val handler = QuestionHandlerFactory(Some("FTB Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> rd))) shouldBe empty
  }

  it should "return empty list when responseDeclaration is an empty JSON object" in {
    val handler = QuestionHandlerFactory(Some("FTB Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> "{}"))) shouldBe empty
  }

  it should "return empty list when extData is None" in {
    val handler = QuestionHandlerFactory(Some("FTB Question")).get
    handler.getAnswers(None) shouldBe empty
  }

  it should "return empty list when responseDeclaration JSON is malformed" in {
    val handler = QuestionHandlerFactory(Some("FTB Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> "{not valid json"))) shouldBe empty
  }

  it should "skip mapping entries that have no value key" in {
    val rd = """{"response1": {"mapping": [{"response": "response1"}]}}"""
    val handler = QuestionHandlerFactory(Some("FTB Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> rd))) shouldBe empty
  }

  it should "return empty string for question when extData is None" in {
    val handler = QuestionHandlerFactory(Some("FTB Question")).get
    handler.getQuestion(None) shouldBe ""
  }

  it should "return empty string for question when body key is absent" in {
    val handler = QuestionHandlerFactory(Some("FTB Question")).get
    handler.getQuestion(Some(Map())) shouldBe ""
  }

  // ─── MTFHandler — happy paths ─────────────────────────────────────────────────

  "MTFHandler" should "extract lhs:rhs pairs from correctResponse.value map" in {
    val rd =
      """{
        |  "response1": {
        |    "correctResponse": {
        |      "value": {
        |        "0": "2",
        |        "1": "3",
        |        "2": "1"
        |      }
        |    }
        |  }
        |}""".stripMargin
    val handler = QuestionHandlerFactory(Some("Match The Following Question")).get
    val answers = handler.getAnswers(Some(Map("responseDeclaration" -> rd)))
    answers should contain allOf ("0:2", "1:3", "2:1")
    answers should have size 3
  }

  it should "return body as the question" in {
    val handler = QuestionHandlerFactory(Some("Match The Following Question")).get
    handler.getQuestion(Some(Map("body" -> "<p>Match the following</p>"))) shouldBe "<p>Match the following</p>"
  }

  // ─── MTFHandler — edge cases ──────────────────────────────────────────────────

  it should "return empty list when responseDeclaration is an empty JSON object" in {
    val handler = QuestionHandlerFactory(Some("Match The Following Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> "{}"))) shouldBe empty
  }

  it should "return empty list when extData is None" in {
    val handler = QuestionHandlerFactory(Some("Match The Following Question")).get
    handler.getAnswers(None) shouldBe empty
  }

  it should "return empty list when responseDeclaration JSON is malformed" in {
    val handler = QuestionHandlerFactory(Some("Match The Following Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> "{not valid json"))) shouldBe empty
  }

  it should "return empty string for question when extData is None" in {
    val handler = QuestionHandlerFactory(Some("Match The Following Question")).get
    handler.getQuestion(None) shouldBe ""
  }

  // ─── SequenceHandler (Sequence Question) — happy paths ───────────────────────

  "SequenceHandler for Sequence Question" should "return values in the declared correct order" in {
    val rd =
      """{
        |  "response1": {
        |    "correctResponse": {
        |      "value": ["3", "1", "4", "2"]
        |    }
        |  }
        |}""".stripMargin
    val handler = QuestionHandlerFactory(Some("Sequence Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> rd))) shouldBe List("3", "1", "4", "2")
  }

  it should "return body as the question" in {
    val handler = QuestionHandlerFactory(Some("Sequence Question")).get
    handler.getQuestion(Some(Map("body" -> "<p>Arrange in order</p>"))) shouldBe "<p>Arrange in order</p>"
  }

  it should "preserve order for a single-element sequence" in {
    val rd = """{"response1": {"correctResponse": {"value": ["1"]}}}"""
    val handler = QuestionHandlerFactory(Some("Sequence Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> rd))) shouldBe List("1")
  }

  // ─── SequenceHandler (Sequence Question) — edge cases ────────────────────────

  it should "return empty list when responseDeclaration is an empty JSON object" in {
    val handler = QuestionHandlerFactory(Some("Sequence Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> "{}"))) shouldBe empty
  }

  it should "return empty list when extData is None" in {
    val handler = QuestionHandlerFactory(Some("Sequence Question")).get
    handler.getAnswers(None) shouldBe empty
  }

  it should "return empty list when responseDeclaration JSON is malformed" in {
    val handler = QuestionHandlerFactory(Some("Sequence Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> "{not valid json"))) shouldBe empty
  }

  it should "return empty string for question when extData is None" in {
    val handler = QuestionHandlerFactory(Some("Sequence Question")).get
    handler.getQuestion(None) shouldBe ""
  }

  // ─── SequenceHandler (Reorder Question) — happy paths ────────────────────────

  "SequenceHandler for Reorder Question" should "return values in the declared correct order" in {
    val rd =
      """{
        |  "response1": {
        |    "correctResponse": {
        |      "value": ["2", "4", "1", "3"]
        |    }
        |  }
        |}""".stripMargin
    val handler = QuestionHandlerFactory(Some("Reorder Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> rd))) shouldBe List("2", "4", "1", "3")
  }

  // ─── SequenceHandler (Reorder Question) — edge cases ─────────────────────────

  it should "return empty list when responseDeclaration is an empty JSON object" in {
    val handler = QuestionHandlerFactory(Some("Reorder Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> "{}"))) shouldBe empty
  }

  it should "return empty list when extData is None" in {
    val handler = QuestionHandlerFactory(Some("Reorder Question")).get
    handler.getAnswers(None) shouldBe empty
  }

  it should "return empty list when responseDeclaration JSON is malformed" in {
    val handler = QuestionHandlerFactory(Some("Reorder Question")).get
    handler.getAnswers(Some(Map("responseDeclaration" -> "{not valid json"))) shouldBe empty
  }

  // ─── apply() dispatch ─────────────────────────────────────────────────────────

  "QuestionHandlerFactory.apply" should "return Some handler for FTB Question" in {
    QuestionHandlerFactory(Some("FTB Question")) shouldBe defined
  }

  it should "return Some handler for Match The Following Question" in {
    QuestionHandlerFactory(Some("Match The Following Question")) shouldBe defined
  }

  it should "return Some handler for Sequence Question" in {
    QuestionHandlerFactory(Some("Sequence Question")) shouldBe defined
  }

  it should "return Some handler for Reorder Question" in {
    QuestionHandlerFactory(Some("Reorder Question")) shouldBe defined
  }

  it should "return None for an unknown question type" in {
    QuestionHandlerFactory(Some("Unknown Type")) shouldBe None
  }

  it should "return None for None input" in {
    QuestionHandlerFactory(None) shouldBe None
  }

  // ─── Regression: pre-existing handlers still dispatch correctly ───────────────

  it should "return Some handler for Multiple Choice Question (regression)" in {
    QuestionHandlerFactory(Some("Multiple Choice Question")) shouldBe defined
  }

  it should "return Some handler for Subjective Question (regression)" in {
    QuestionHandlerFactory(Some("Subjective Question")) shouldBe defined
  }
}
