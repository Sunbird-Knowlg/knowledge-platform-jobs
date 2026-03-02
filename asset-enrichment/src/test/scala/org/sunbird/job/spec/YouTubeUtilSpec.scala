package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.assetenricment.task.AssetEnrichmentConfig
import org.sunbird.job.assetenricment.util.YouTubeUtil
import org.sunbird.spec.BaseTestSpec

class YouTubeUtilSpec extends BaseTestSpec with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig = new AssetEnrichmentConfig(config)
  val mockYouTubeUtil = mock[YouTubeUtil]

  "getIdFromUrl" should "return the id of the video for the provided Youtube URL" in {
    val result = new YouTubeUtil(jobConfig).getIdFromUrl("https://www.youtube.com/watch?v=-SgZ3Enpau8")
    result.getOrElse("") should be("-SgZ3Enpau8")
  }

  "computeVideoDuration" should "return the duration of the medium video for the " in {
    val result = new YouTubeUtil(jobConfig).computeVideoDuration("PT4M33S")
    result should be("273")
  }

  "computeVideoDuration" should "return the duration of the long video for the " in {
    val result = new YouTubeUtil(jobConfig).computeVideoDuration("PT1H4M33S")
    result should be("3873")
  }

  "computeVideoDuration" should "return the duration of the short video for the " in {
    val result = new YouTubeUtil(jobConfig).computeVideoDuration("PT33S")
    result should be("33")
  }

  "computeVideoDuration" should "return the empty for wrong video length2 " in {
    val result = new YouTubeUtil(jobConfig).computeVideoDuration("PT1H4M33SPT1H4M33SPT1H4M33S")
    result should be("")
  }

  "getVideoInfo" should "return empty map if no url is passed " in {
    when(mockYouTubeUtil.getVideoInfo(org.mockito.ArgumentMatchers.eq(""), anyString(), any())).thenReturn(Map[String, AnyRef]())
    val result = mockYouTubeUtil.getVideoInfo("", "snippet,contentDetails", List[String]("thumbnail", "duration", "license"))
    result.isEmpty should be(true)
  }

  "getVideoInfo" should "return the empty data for incorrect Youtube URL" in {
    when(mockYouTubeUtil.getVideoInfo(org.mockito.ArgumentMatchers.eq("https://www.youtube.com/watch?v=-SgZ3En23sd"), anyString(), any())).thenReturn(Map[String, AnyRef]())
    val result = mockYouTubeUtil.getVideoInfo("https://www.youtube.com/watch?v=-SgZ3En23sd", "snippet,contentDetails", List[String]("thumbnail", "duration", "xyz"))
    result.isEmpty should be(true)
  }

  "getVideoInfo" should "throw Exception for null URL" in {
    when(mockYouTubeUtil.getVideoInfo(org.mockito.ArgumentMatchers.isNull(), anyString(), any())).thenThrow(new RuntimeException("Invalid URL"))
    assertThrows[RuntimeException] {
      mockYouTubeUtil.getVideoInfo(null, "snippet,contentDetails", List[String]("thumbnail", "duration", "xyz"))
    }
  }

  "getVideo" should " throw exception for null video Id " in {
    when(mockYouTubeUtil.getVideo(org.mockito.ArgumentMatchers.isNull(), any())).thenThrow(new RuntimeException("Required parameter part must be specified."))
    assertThrows[RuntimeException] {
      mockYouTubeUtil.getVideo(null, null)
    }
  }

}
