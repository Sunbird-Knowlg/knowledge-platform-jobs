package org.sunbird.job.publish.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.publish.helpers.ThumbnailGenerator
import org.sunbird.job.util.CloudStorageUtil


import org.mockito.ArgumentMatchers.{any, anyBoolean, anyString}
import org.mockito.Mockito.{doNothing, when}
import org.scalatestplus.mockito.MockitoSugar
import java.io.File

class ThumbnailGeneratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    when(cloudStorageUtil.uploadFile(any(), any(), any(), any())).thenReturn(Array("test-key", "test-url"))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val publishConfig: PublishConfig = new PublishConfig(config, "")
  implicit val cloudStorageUtil: CloudStorageUtil = mock[CloudStorageUtil]

  "Object Thumbnail Generator generateThumbnail" should "add the thumbnail to ObjectData" in {

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val metadata = Map("identifier" -> "do_123", "appIcon" -> "https://www.google.com/images/branding/googlelogo/2x/googlelogo_color_272x92dp.png", "IL_UNIQUE_ID" -> "do_123", "objectType" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))

    val thumbnailGenerator = new TestThumbnailGenerator()
    val obj = thumbnailGenerator.generateThumbnail(objData)
    val result = obj.getOrElse(objData)
    val resultMetadata = result.metadata
    resultMetadata.isEmpty should be(false)
    resultMetadata.getOrElse("posterImage", "").asInstanceOf[String].isEmpty should be(false)
    resultMetadata.getOrElse("appIcon", "").asInstanceOf[String].isEmpty should be(false)
    resultMetadata.getOrElse("posterImage", "").asInstanceOf[String] shouldBe "https://www.google.com/images/branding/googlelogo/2x/googlelogo_color_272x92dp.png"
    resultMetadata.getOrElse("appIcon", "").asInstanceOf[String] shouldBe "test-url"
  }

  "Object Thumbnail Generator generateThumbnail with google drive link" should "add the thumbnail to ObjectData" in {
    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val metadata = Map("identifier" -> "do_123", "appIcon" -> "https://www.google.com/images/branding/googlelogo/2x/googlelogo_color_272x92dp.png", "IL_UNIQUE_ID" -> "do_123", "objectType" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))
    val thumbnailGenerator = new TestThumbnailGenerator()
    val obj = thumbnailGenerator.generateThumbnail(objData)
    val result = obj.getOrElse(objData)
    val resultMetadata = result.metadata
    resultMetadata.isEmpty should be(false)
    resultMetadata.getOrElse("posterImage", "").asInstanceOf[String].isEmpty should be(false)
    resultMetadata.getOrElse("appIcon", "").asInstanceOf[String].isEmpty should be(false)
    resultMetadata.getOrElse("posterImage", "").asInstanceOf[String] shouldBe "https://www.google.com/images/branding/googlelogo/2x/googlelogo_color_272x92dp.png"
    resultMetadata.getOrElse("appIcon", "").asInstanceOf[String] shouldBe "test-url"
  }

}

class TestThumbnailGenerator extends ThumbnailGenerator {

}