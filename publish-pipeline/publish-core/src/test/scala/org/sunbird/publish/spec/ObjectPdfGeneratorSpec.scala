package org.sunbird.publish.spec


import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.util.{HTTPResponse, HttpUtil}
import org.sunbird.publish.config.PublishConfig
import org.sunbird.publish.core.ObjectData
import org.sunbird.publish.helpers.QuestionPdfGenerator
import org.sunbird.publish.util.CloudStorageUtil


class ObjectPdfGeneratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

    override protected def beforeAll(): Unit = {
        super.beforeAll()
    }

    override protected def afterAll(): Unit = {
        super.afterAll()
    }

    implicit val mockHttpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
    implicit val publishConfig: PublishConfig = new PublishConfig(config, "")

    implicit val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(publishConfig)


    "Object Pdf Generator getPreviewFileUrl" should "return a url of the html file after uploading it to cloud" in {
        val pdfGenerator = new TestQuestionPdfGenerator()
        val obj = pdfGenerator.getPreviewFileUrl(getObjectList(), getObject(), "questionSetTemplate.vm")
        obj.getOrElse("").isEmpty should be(false)
    }

    "Object PDF generator getPdfFileUrl" should "return a url of the pdf file after uploading it to cloud" in {
        when(mockHttpUtil.post(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(getHttpResponse())
        val pdfGenerator = new TestQuestionPdfGenerator()
        val (pdfUrl, previewUrl) = pdfGenerator.getPdfFileUrl(getObjectList(), getObject(), "questionSetTemplate.vm", "http://11.2.6.6/print")
        pdfUrl.getOrElse("").isEmpty should be(false)
        previewUrl.getOrElse("").isEmpty should be(false)

    }

    private def getObjectList(): List[ObjectData] = {
        val question_1 = new ObjectData("do_123", Map("primaryCategory" -> "Multiple Choice Question", "index" -> 1.asInstanceOf[AnyRef], "IL_UNIQUE_ID" -> "do_123"),
            Some(Map(
                "responseDeclaration" ->
                    """
                      |{
                      |        "response1": {
                      |          "maxScore": 1,
                      |          "cardinality": "single",
                      |          "type": "integer",
                      |          "correct_response": {
                      |            "value": 1,
                      |            "outcomes": {
                      |              "score": 1
                      |            }
                      |          }
                      |        }
                      |    }
                    """.stripMargin,
                "interactions" ->
                    """
                      |{
                      |        "response1": {
                      |          "type": "choice",
                      |          "options": [
                      |            {
                      |              "body": "<p>2 September 1929</p>",
                      |              "value": 0
                      |            },
                      |            {
                      |              "body": "<p>15 October 1931</p>",
                      |              "value": 1
                      |            },
                      |            {
                      |              "body": "<p>15 August 1923</p>",
                      |              "value": 2
                      |            },
                      |            {
                      |              "body": "<p>29 February 1936</p>",
                      |              "value": 3
                      |            }
                      |          ]
                      |        }
                      |    }
                    """.stripMargin,
                "body" ->
                    """<div class='question-body'>
                      |      <div class='question-title'>When was Dr. A.P.J. Abdul Kalam born</div>
                      |      <div data-choice-interaction='response1' class='mcq-vertical'></div>
                      |    </div>""")), None)
        val question_2 = new ObjectData("do_234", Map("primaryCategory" -> "Multiple Choice Question", "index" -> 2.asInstanceOf[AnyRef], "IL_UNIQUE_ID" -> "do_234"),
            Some(Map(
                "responseDeclaration" ->
                    """
                      |	{
                      |	  "maxScore": 3,
                      |      "response1": {
                      |        "cardinality": "multiple",
                      |        "type": "integer",
                      |        "correct_response": {
                      |          "value": [
                      |            0,
                      |            1,
                      |            2
                      |          ],
                      |          "outcomes": {
                      |            "score": 3
                      |          }
                      |        },
                      |        "mapping": [
                      |          {
                      |            "response": 0,
                      |            "outcomes": {
                      |              "score": 1
                      |            }
                      |          },
                      |          {
                      |            "response": 1,
                      |            "outcomes": {
                      |              "score": 1
                      |            }
                      |          },
                      |          {
                      |            "response": 2,
                      |            "outcomes": {
                      |              "score": 1
                      |            }
                      |          }
                      |        ]
                      |      }
                      |    }
                    """.stripMargin,
                "interactions" ->
                    """		{
                      |        "response1": {
                      |          "type": "choice",
                      |          "options": [
                      |            {
                      |              "body": "<p>Failure to Success: Legendary Lives</p>",
                      |              "value": 0
                      |            },
                      |            {
                      |              "body": "<p>You Are Born to Blossom</p>",
                      |              "value": 1
                      |            },
                      |            {
                      |              "body": "<p>Ignited Minds</p>",
                      |              "value": 2
                      |            },
                      |            {
                      |              "body": "<p>A House for Mr. Biswas‎ </p>",
                      |              "value": 3
                      |            }
                      |          ]
                      |        }
                      |    }
                      |    """.stripMargin,
                "body" ->
                    """
                      |<div class='question-body'>
                      |      <div class='question-title'>Which of the following books is written by Dr. A.P.J. Abdul Kalam</div>
                      |      <div multiple data-choice-interaction='response1' class='mcq-vertical'></div>
                      |    </div>
                    """.stripMargin)))

        val question_3 = new ObjectData("do_345", Map("primaryCategory" -> "Subjective Question", "index" -> 3.asInstanceOf[AnyRef], "IL_UNIQUE_ID" -> "do_345"),
            Some(Map(
                "body" -> " <div>The tenure of APJ Abdul Kalam as Indian President</div>",
                "answer" -> " <div>2002 to 2007</div>"
            )))
        List(question_1, question_2, question_3)
    }

    private def getObject(): ObjectData = {
        new ObjectData("do_xyz", Map("name" -> "Test Question Set", "IL_UNIQUE_ID" -> "do_xyz"))
    }

    private def getHttpResponse(): HTTPResponse = {
        HTTPResponse(200,
            """
              |{
              |"result" : {
              |     "pdfUrl" : "https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11304066349776076815/artifact/do_11304066349776076815_1591877926475.pdf"
              |}
              |}""".stripMargin)
    }

}

class TestQuestionPdfGenerator extends QuestionPdfGenerator {


}
