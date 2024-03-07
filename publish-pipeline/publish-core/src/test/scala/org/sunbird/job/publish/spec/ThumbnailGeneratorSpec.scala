package org.sunbird.job.publish.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.publish.helpers.ThumbnailGenerator
import org.sunbird.job.util.CloudStorageUtil


class ThumbnailGeneratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val publishConfig: PublishConfig = new PublishConfig(config, "")
  implicit val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(publishConfig)

  "Object Thumbnail Generator generateThumbnail" should "add the thumbnail to ObjectData" in {

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val metadata = Map("identifier" -> "do_123", "appIcon" -> "https://dev.sunbirded.org/content/preview/assets/icons/avatar_anonymous.png", "IL_UNIQUE_ID" -> "do_123", "objectType" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))

    val thumbnailGenerator = new TestThumbnailGenerator()
    val obj = thumbnailGenerator.generateThumbnail(objData)
    val result = obj.getOrElse(objData)
    val resultMetadata = result.metadata
    resultMetadata.isEmpty should be(false)
    resultMetadata.getOrElse("posterImage", "").asInstanceOf[String].isEmpty should be(false)
    resultMetadata.getOrElse("appIcon", "").asInstanceOf[String].isEmpty should be(false)
    resultMetadata.getOrElse("posterImage", "").asInstanceOf[String] shouldBe "https://dev.knowlg.sunbird.org/content/preview/assets/icons/avatar_anonymous.png"
    resultMetadata.getOrElse("appIcon", "").asInstanceOf[String] shouldBe "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/questionset/do_123/artifact/avatar_anonymous.thumb.png"
  }

  "Object Thumbnail Generator generateThumbnail with google drive link" should "add the thumbnail to ObjectData" in {
    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val metadata = Map("identifier" -> "do_123", "appIcon" -> "https://drive.google.com/uc?export=download&id=1-dFzAeSNmx1ZRn77CEntyQA-VcBE0PKg", "IL_UNIQUE_ID" -> "do_123", "objectType" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))
    val thumbnailGenerator = new TestThumbnailGenerator()
    val obj = thumbnailGenerator.generateThumbnail(objData)
    val result = obj.getOrElse(objData)
    val resultMetadata = result.metadata
    resultMetadata.isEmpty should be(false)
    resultMetadata.getOrElse("posterImage", "").asInstanceOf[String].isEmpty should be(false)
    resultMetadata.getOrElse("appIcon", "").asInstanceOf[String].isEmpty should be(false)
    resultMetadata.getOrElse("posterImage", "").asInstanceOf[String] shouldBe "https://drive.google.com/uc?export=download&id=1-dFzAeSNmx1ZRn77CEntyQA-VcBE0PKg"
    resultMetadata.getOrElse("appIcon", "").asInstanceOf[String] shouldBe "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/questionset/do_123/artifact/book.thumb.jpg"
  }

}

class TestThumbnailGenerator extends ThumbnailGenerator {

}