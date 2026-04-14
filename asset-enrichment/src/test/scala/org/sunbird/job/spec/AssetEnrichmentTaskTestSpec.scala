package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.doNothing
import org.sunbird.job.assetenricment.domain.Event
import org.sunbird.job.assetenricment.functions.{ImageEnrichmentFunction, VideoEnrichmentFunction}
import org.sunbird.job.assetenricment.models.Asset
import org.sunbird.job.assetenricment.task.AssetEnrichmentConfig
import org.sunbird.job.assetenricment.util.{AssetFileUtils, ImageResizerUtil, YouTubeUtil}
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.util.{CloudStorageUtil, FileUtils, JSONUtil, JanusGraphUtil, ScalaJsonUtil}
import org.sunbird.spec.BaseTestSpec

import java.io.File
import java.util

class AssetEnrichmentTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val strTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig = new AssetEnrichmentConfig(config)
  val definitionUtil = new DefinitionCache
  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit val cloudUtil: CloudStorageUtil = new CloudStorageUtil(jobConfig)
  implicit val youTubeUtil: YouTubeUtil = new YouTubeUtil(jobConfig)
  val imagePath = config.getString("blob.input.contentImagePath")
  val videoPath = config.getString("blob.input.contentVideoPath")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }

  "enrichImage()" should " enrich the image for the asset " in {
    doNothing().when(mockJanusGraphUtil).updateNode(anyString(), any[Map[String, AnyRef]]())

    val contentId = imagePath.split("/").dropWhile(_ != "content").drop(1).headOption.getOrElse("")
    val metadata = getMetadataForImageAsset
    val updatedMetadata = metadata.updated("cloudStorageKey", s"content/$contentId/artifact/${imagePath.split("/").last}")
    
    val asset = getAsset(EventFixture.IMAGE_ASSET, updatedMetadata)
    new ImageEnrichmentFunction(jobConfig).enrichImage(asset)(jobConfig, definitionUtil, cloudUtil, mockJanusGraphUtil)
    val variants = ScalaJsonUtil.deserialize[Map[String, String]](asset.get("variants", "").asInstanceOf[String])
    variants.size should be(3)
    variants.keys should contain allOf("high", "medium", "low")
    // The high variant should have .high before the file extension
    val expectedHighVariant = imagePath.replace(".jpg", ".high.jpg")
    variants.getOrElse("high", "") should be(expectedHighVariant)
    asset.get("status", "").asInstanceOf[String] should be("Live")
  }

  "videoEnrichment" should " enrich the video for the mp4 asset " in {
    doNothing().when(mockJanusGraphUtil).updateNode(anyString(), any[Map[String, AnyRef]]())

    val metadata = getMetadataForMp4VideoAsset
    val asset = getAsset(EventFixture.VIDEO_MP4_ASSET, metadata)
    new VideoEnrichmentFunction(jobConfig).enrichVideo(asset)(jobConfig, youTubeUtil, cloudUtil, mockJanusGraphUtil)
    asset.get("status", "").asInstanceOf[String] should be("Live")
  }

  "videoEnrichment" should " enrich the video for the youtube asset " in {
    doNothing().when(mockJanusGraphUtil).updateNode(anyString(), any[Map[String, AnyRef]]())

    val metadata = getMetadataForYouTubeVideoAsset
    val asset = getAsset(EventFixture.VIDEO_YOUTUBE_ASSET, metadata)
    new VideoEnrichmentFunction(jobConfig).enrichVideo(asset)(jobConfig, youTubeUtil, cloudUtil, mockJanusGraphUtil)
    asset.get("thumbnail", "").asInstanceOf[String] should be("https://i.ytimg.com/vi/-SgZ3Enpau8/mqdefault.jpg")
    asset.get("status", "").asInstanceOf[String] should be("Live")
    asset.get("duration", "0").asInstanceOf[String] should be("274")
  }

  "validateForArtifactUrl" should "validate for content upload context driven" in {
    val metadata = Map[String, AnyRef]("cloudStorageKey" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1132316405761064961124/artifact/0_jmrpnxe-djmth37l_.jpg",
      "s3Key" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1132316405761064961124/artifact/0_jmrpnxe-djmth37l_.jpg",
      "artifactBasePath" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev",
      "artifactUrl" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_1132316405761064961124/artifact/0_jmrpnxe-djmth37l_.jpg")
    val asset = getAsset(EventFixture.IMAGE_ASSET, metadata)
    val validate = asset.validate(true)
    validate should be(true)
  }

  "ImageEnrichmentHelper" should "return Exception if the image url is wrong" in {
    val metadata = getMetadataForImageAsset
    val asset = getAsset(EventFixture.IMAGE_ASSET, metadata)
    asset.put("artifactUrl", "https://unknownurl123.com")
    assertThrows[Exception] {
      new ImageEnrichmentFunction(jobConfig).enrichImage(asset)(jobConfig, definitionUtil, cloudUtil, mockJanusGraphUtil)
    }
  }

  "VideoEnrichmentHelper" should "return Exception if the video url is wrong" in {
    val metadata = getMetadataForMp4VideoAsset
    val asset = getAsset(EventFixture.VIDEO_MP4_ASSET, metadata)
    asset.put("artifactUrl", "https://unknownurl1234.com")
    assertThrows[Exception] {
      new VideoEnrichmentFunction(jobConfig).enrichVideo(asset)(jobConfig, youTubeUtil, cloudUtil, mockJanusGraphUtil)
    }
  }

  "VideoEnrichmentHelper" should "return Exception if the video url is empty" in {
    val metadata = getMetadataForMp4VideoAsset
    val asset = getAsset(EventFixture.VIDEO_MP4_ASSET, metadata)
    asset.put("artifactUrl", "")
    assertThrows[Exception] {
      new VideoEnrichmentFunction(jobConfig).enrichVideo(asset)(jobConfig, youTubeUtil, cloudUtil, mockJanusGraphUtil)
    }
  }

  "VideoEnrichmentFunction" should "return Exception if the metadata is not found" in {
    val metadata = getMetadataForMp4VideoAsset
    val asset = getAsset(EventFixture.VIDEO_MP4_ASSET, metadata)
    asset.put("artifactUrl", "")
    assertThrows[Exception] {
      new VideoEnrichmentFunction(jobConfig).getMetadata(asset.identifier)(mockJanusGraphUtil)
    }
  }

  "ImageEnrichmentFunction" should "return Exception if the metadata is not found" in {
    val metadata = getMetadataForImageAsset
    val asset = getAsset(EventFixture.IMAGE_ASSET, metadata)
    asset.put("artifactUrl", "")
    assertThrows[Exception] {
      new ImageEnrichmentFunction(jobConfig).getMetadata(asset.identifier)(mockJanusGraphUtil)
    }
  }

  "getVideoInfo" should "throw exception if video Url is empty" in {
    val metadata = getMetadataForYouTubeVideoAsset
    val asset = getAsset(EventFixture.VIDEO_YOUTUBE_ASSET, metadata)
    assertThrows[Exception] {
      new VideoEnrichmentFunction(jobConfig).processYoutubeVideo(asset, "")(youTubeUtil)
    }
  }

  "event.validate" should " validate the event " in {
    val event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.IMAGE_ASSET), 0, 10)
    val message = event.validate(jobConfig.maxIterationCount)
    message.isEmpty should be(true)
  }

  "getStreamingEvent" should "return event string for streaming for video assset" in {
    val metadata = getMetadataForMp4VideoAsset
    val asset = getAsset(EventFixture.VIDEO_MP4_ASSET, metadata)
    val eventMsg = new VideoEnrichmentFunction(jobConfig).getStreamingEvent(asset)(jobConfig)
    eventMsg.isEmpty should be(false)
  }

  "upload" should " throw exception for incorrect file in ImageEnrichment " in {
    assertThrows[Exception] {
      new ImageEnrichmentFunction(jobConfig).upload(null, "do_123")(cloudUtil)
    }
  }

  "upload" should " throw exception for incorrect file in VideoEnrichment " in {
    assertThrows[Exception] {
      new VideoEnrichmentFunction(jobConfig).upload(null, "do_123")(cloudUtil)
    }
  }

  "ImageResizerUtil" should " replace the provided files " in {
    val imageUrl = s"$imagePath"
    val contentId = imagePath.split("/").dropWhile(_ != "content").drop(1).headOption.getOrElse("unknown")
    val fileName = imagePath.substring(imagePath.lastIndexOf("/") + 1)
    try {
      val file = FileUtils.copyURLToFile(contentId, imageUrl, fileName)
      val newFile = new File(s"/tmp/$contentId/1617194149349_temp/${fileName.replace(".jpg", ".jpg")}")
      val result = new ImageResizerUtil().replace(file.get, newFile)
      result.getAbsolutePath should endWith(fileName.replace(".jpg", ".jpg"))
    } finally {
      FileUtils.deleteDirectory(new File(s"/tmp/$contentId"))
    }
  }

  def getAsset(event: String, metadata: Map[String, AnyRef]): Asset = {
    val eventMap = JSONUtil.deserialize[util.Map[String, Any]](event)
    val asset = Asset(eventMap)
    asset.putAll(metadata)
    asset
  }

  def getMetadataForImageAsset: Map[String, AnyRef] = {
    val contentId = imagePath.split("/").dropWhile(_ != "content").drop(1).headOption.getOrElse("unknown")
    val fileName = imagePath.split("/").lastOption.getOrElse("unknown")
    val metadata = s"""{"ownershipType": ["createdBy"], "code": "org.ekstep0.07321483804683715", "prevStatus": "Processing", "downloadUrl": "$imagePath", "channel": "b00bc992ef25f1a9a8d63291e20efc8d", "language": ["English"], "mimeType": "image/jpeg","idealScreenSize": "normal", "createdOn": "2021-03-11T06:27:08.370+0000", "primaryCategory": "Certificate Template", "contentDisposition": "inline", "artifactUrl": "$imagePath", "contentEncoding": "identity", "lastUpdatedOn": "2021-03-11T06:27:10.254+0000", "contentType": "Asset", "dialcodeRequired": "No", "audience": ["Student"], "creator": "Reviewer User", "lastStatusChangedOn": "2021-03-11T06:27:10.237+0000", "visibility": "Default", "os": ["All"], "IL_SYS_NODE_TYPE": "DATA_NODE", "cloudStorageKey": "content/$contentId/artifact/$fileName", "mediaType": "image", "osId": "org.ekstep.quiz.app", "version": 2, "versionKey": "1615444030254", "license": "CC BY 4.0", "idealScreenDensity": "hdpi", "s3Key": "content/$contentId/artifact/$fileName", "framework": "NCF", "size": 86402.0, "createdBy": "95e4942d-cbe8-477d-aebd-ad8e6de4bfc8", "compatibilityLevel": 1, "IL_FUNC_OBJECT_TYPE": "Asset", "name": "$fileName", "IL_UNIQUE_ID": "$contentId", "status": "Live"}"""
    ScalaJsonUtil.deserialize[Map[String, AnyRef]](metadata)
  }

  def getMetadataForMp4VideoAsset: Map[String, AnyRef] = {
    val metadata = s"""{"artifactUrl": "$videoPath", "ownershipType": ["createdBy"], "code": "Test_Asset_15_MB", "apoc_json": "{\\"batch\\": true}", "channel": "in.ekstep", "organisation": ["Sunbird", "QA ORG"], "language": ["English"], "mimeType": "video/mp4", "media": "video", "idealScreenSize": "normal", "createdOn": "2019-03-06T13:13:13.917+0000", "apoc_text": "APOC", "primaryCategory": "Asset", "appId": "local.sunbird.portal", "contentDisposition": "inline", "contentEncoding": "identity", "lastUpdatedOn": "2019-03-06T13:13:13.917+0000", "contentType": "Asset", "dialcodeRequired": "No", "apoc_num": 1, "audience": ["Learner"], "creator": "Creation", "createdFor": ["ORG_001", "0123653943740170242"], "visibility": "Default", "os": ["All"], "IL_SYS_NODE_TYPE": "DATA_NODE", "consumerId": "9393568c-3a56-47dd-a9a3-34da3c821638", "mediaType": "content", "osId": "org.ekstep.quiz.app", "versionKey": "1551877993917", "idealScreenDensity": "hdpi", "framework": "NCFCOPY", "createdBy": "874ed8a5-782e-4f6c-8f36-e0288455901e", "compatibilityLevel": 1.0, "IL_FUNC_OBJECT_TYPE": "Asset", "name": "Test_Asset_15_MB", "IL_UNIQUE_ID": "do_1127129845261680641588", "status": "Draft"}"""
    ScalaJsonUtil.deserialize[Map[String, AnyRef]](metadata)
  }

  def getMetadataForYouTubeVideoAsset: Map[String, AnyRef] = {
    val metadata = """{"artifactUrl": "https://www.youtube.com/watch?v=-SgZ3Enpau8", "ownershipType": ["createdBy"], "code": "Test_Asset_15_MB", "apoc_json": "{\"batch\": true}", "channel": "in.ekstep", "organisation": ["Sunbird", "QA ORG"], "language": ["English"], "mimeType": "video/x-youtube", "media": "video", "idealScreenSize": "normal", "createdOn": "2019-03-06T13:13:13.917+0000", "apoc_text": "APOC", "primaryCategory": "Asset", "appId": "local.sunbird.portal", "contentDisposition": "inline", "contentEncoding": "identity", "lastUpdatedOn": "2019-03-06T13:13:13.917+0000", "contentType": "Asset", "dialcodeRequired": "No", "apoc_num": 1, "audience": ["Learner"], "creator": "Creation", "createdFor": ["ORG_001", "0123653943740170242"], "visibility": "Default", "os": ["All"], "IL_SYS_NODE_TYPE": "DATA_NODE", "consumerId": "9393568c-3a56-47dd-a9a3-34da3c821638", "mediaType": "content", "osId": "org.ekstep.quiz.app", "versionKey": "1551877993917", "idealScreenDensity": "hdpi", "framework": "NCFCOPY", "createdBy": "874ed8a5-782e-4f6c-8f36-e0288455901e", "compatibilityLevel": 1.0, "IL_FUNC_OBJECT_TYPE": "Asset", "name": "Test_Asset_15_MB", "IL_UNIQUE_ID": "do_1127129845261680641599", "status": "Draft"}"""
    ScalaJsonUtil.deserialize[Map[String, AnyRef]](metadata)
  }

}
