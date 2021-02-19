package org.sunbird.publish.spec

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.publish.core.ObjectData
import org.sunbird.publish.helpers.ObjectTemplateGenerator


class ObjectTemplateGeneratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

    override protected def beforeAll(): Unit = {
        super.beforeAll()
    }

    override protected def afterAll(): Unit = {
        super.afterAll()
    }

    "Object Template Generator " should "generate and return a template string" in {
        val objValidator = new TestTemplateGenerator()
        val htmlTemplateString = objValidator.handleHtmlTemplate("questionSetTemplate.vm", getTemplateContext)
        htmlTemplateString.isEmpty should be(false)
        htmlTemplateString shouldEqual("<header> \n<style type=\"text/css\">\n\tbody { \n\t\tpadding: 0; \n\t\tmargin: 0; \n\t}\n\tp { \n\t\tmargin: 0; \n\t}\n\t.questions-paper { \n\t\tpadding: 50px; \n\t}\n\t.question-header { \n\t\ttext-align: center; \n\t\tpadding: 16px 0; \n\t\tborder-bottom: 1px solid #999; \n\t}\n\t.question-header h2 { \n\t\tmargin: 0; \n\t}\n\t.main-container .question-section { \n\t\tdisplay: flex; \n\t\tpadding-top: 16px; \n\t}\n\t.main-container .question-section:last-child { \n\t\tdisplay: flex; \n\t\tpadding-bottom: 16px; \n\t}\n\t.question-count { \n\t\tfont-weight: 600; \n\t}\n\t.question-content { \n\t\tpadding: 0 8px; \n\t}\n\t.question-title { \n\t\tfont-size: 16px; \n\t\tfont-weight: 600; \n\t\tpadding-bottom: 8px; \n\t}\n\t.mcq-option { \n\t\tpadding-left: 16px; \n\t}\n\t.mcq-option p { \n\t\tline-height: 24px; \n\t}\n\t.mcq-option p:before { \n\t\tcontent: '\\2022';\n\t\tmargin-right: 8px; \n\t}\n\t.answer { \n\t\tpadding-left: 20px; \n\t}\n\t.answer p:before { \n\t\tcontent: '\\2022';\n\t\tmargin-right: 8px; \n\t}\n\t.answer-sheet { \n\t\tpage-break-before: always; \n\t}\n\t.question-section {\n\t    page-break-inside: avoid;\n\t}\n</style> \n</header>\n\n<div class=\"questions-paper\">\n<div class=\"question-sheet\">\n    <div class=\"question-header\"><h2>$title</h2></div>\n    <div class=\"main-container\">\n<div class='question-section'>\n<div class='question-count'>\n1.\nWhat is the color of the sky?\n</div>\n</div>\n<div class='question-section'>\n<div class='question-count'>\n2.\nWhat is the color of the leaf?\n</div>\n</div>\n            </div>\n</div>\n<div class=\"answer-sheet\">\n    <div class=\"question-header\"><h2>Answers</h2></div>\n    <div class=\"main-container\">\n                  |<div class='question-section'>\n                  |<div class='question-count'>\n                  |1.\n                  |</div>\n                  |What is the color of the sky?\n                  |</div>\n                  |<div class='answer'>\n                  |blue\n                  |</div>\n                  |<div class='question-section'>\n                  |<div class='question-count'>\n                  |1.\n                  |</div>\n                  |What is the color of the leaf?\n                  |</div>\n                  |<div class='answer'>\n                  |grean\n                  |</div>\n                </div>\n</div>\n\n</div>")
    }


    def getTemplateContext = {
        Map("questions" ->
            """
              |<div class='question-section'>
              |<div class='question-count'>
              |1.
              |What is the color of the sky?
              |</div>
              |</div>
              |<div class='question-section'>
              |<div class='question-count'>
              |2.
              |What is the color of the leaf?
              |</div>
              |</div>
            """.stripMargin,
            "answers" ->
                """
                  |<div class='question-section'>
                  |<div class='question-count'>
                  |1.
                  |</div>
                  |What is the color of the sky?
                  |</div>
                  |<div class='answer'>
                  |blue
                  |</div>
                  |<div class='question-section'>
                  |<div class='question-count'>
                  |1.
                  |</div>
                  |What is the color of the leaf?
                  |</div>
                  |<div class='answer'>
                  |grean
                  |</div>
                """)
    }

}

class TestTemplateGenerator extends ObjectTemplateGenerator {


}
