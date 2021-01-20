package org.sunbird.publish.spec

import java.io.File

import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, Neo4JUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}
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
    implicit val cloudStorageUtil: CloudStorageUtil = mock[CloudStorageUtil](Mockito.withSettings().serializable())

    "Object Pdf Generator getPreviewFileUrl " should " return a url of the html file after uploading it to cloud " in {
        when(cloudStorageUtil.uploadFile("test", new File(""), Some(true))).thenReturn(Array("122132", "https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11304066349776076815/artifact/do_11304066349776076815_1591877926475.pdf"))
        val pdfGenerator = new TestQuestionPdfGenerator()
        val obj = pdfGenerator.getPreviewFileUrl(getObjectList(), getObject(), "questionSetTemplate.vm")
        obj.getOrElse("").isEmpty should be(false)
    }

    "Object PDF generator getPdfFileUrl" should "return a url of the pdf file after uploading it to cloud" ignore {
        when(mockHttpUtil.post("", "")).thenReturn(getHttpResponse())
        when(cloudStorageUtil.uploadFile("test", new File(""), Some(true))).thenReturn(Array("122132", "https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11304066349776076815/artifact/do_11304066349776076815_1591877926475.pdf"))
        val pdfGenerator = new TestQuestionPdfGenerator()
        val obj = pdfGenerator.getPdfFileUrl(getObjectList(), getObject(), "questionSetTemplate.vm")
        obj.getOrElse("").isEmpty should be(false)
    }

    private def getObjectList(): List[ObjectData] = {
        val question_1 = new ObjectData("do_123", Map("index" -> 1.asInstanceOf[AnyRef]),
            Some(Map("body" -> "<div class='question-body'><div class='mcq-title'><p>Color of Sky is?</p></div><div data-choice-interaction='response1' class='mcq-vertical'></div></div>",
                "editorState" ->
                    """
                      |{
                      |                "options": [
                      |                    {
                      |                        "answer": false,
                      |                        "value": {
                      |                            "body": "<p>Red&nbsp;</p><figure class=\"image\"><img src=\"https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11318931140144332811620/artifact/i.png\" alt=\"do_11318931140144332811620\" data-asset-variable=\"do_11318931140144332811620\"></figure>",
                      |                            "value": 0
                      |                        }
                      |                    },
                      |                    {
                      |                        "answer": true,
                      |                        "value": {
                      |                            "body": "<p>Blue&nbsp;</p><figure class=\"image\"><img src=\"https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11318931140144332811620/artifact/i.png\" alt=\"do_11318931140144332811620\" data-asset-variable=\"do_11318931140144332811620\"></figure>",
                      |                            "value": 1
                      |                        }
                      |                    },
                      |                    {
                      |                        "answer": false,
                      |                        "value": {
                      |                            "body": "<p>Green&nbsp;</p><figure class=\"image\"><img src=\"https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11318931140144332811620/artifact/i.png\" alt=\"do_11318931140144332811620\" data-asset-variable=\"do_11318931140144332811620\"></figure>",
                      |                            "value": 2
                      |                        }
                      |                    },
                      |                    {
                      |                        "answer": false,
                      |                        "value": {
                      |                            "body": "<p>Yellow&nbsp;</p><figure class=\"image\"><img src=\"https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11318931140144332811620/artifact/i.png\" alt=\"do_11318931140144332811620\" data-asset-variable=\"do_11318931140144332811620\"></figure>",
                      |                            "value": 3
                      |                        }
                      |                    }
                      |                ],
                      |                "question": "<p>Color of Sky is?</p>",
                      |                "solutions": [
                      |                    {
                      |                        "id": "c012a8a9-f78b-6ddb-3ac2-bd1f38c7850b",
                      |                        "type": "html",
                      |                        "value": "<p>Color of sky is blue&nbsp;</p><figure class=\"image\"><img src=\"https://dockstorage.blob.core.windows.net/sunbird-content-dock/content/do_11310507846892748812026/artifact/icon.png\" alt=\"do_11310507846892748812026\" data-asset-variable=\"do_11310507846892748812026\"></figure>"
                      |                    }
                      |                ]
                      |            }
                    """.stripMargin)))
        val question_2 = new ObjectData("do_234", Map("index" -> 2.asInstanceOf[AnyRef]),
            Some(Map("body" -> "<p>Capital of india is?</p>",
                "editorState" ->
                    """
                      |{
                      |                "question": "<p>Capital of india is?</p>",
                      |                "answer": "<p>New Delhi</p>"
                      |            }
                    """.stripMargin)))
        List(question_1, question_2)
    }

    private def getObject(): ObjectData = {
        new ObjectData("do_xyz", Map("name" -> "Test Question Set"))
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
