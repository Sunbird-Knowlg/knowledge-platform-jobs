package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.sunbird.job.assetenricment.task.AssetEnrichmentConfig
import org.sunbird.job.assetenricment.util.YouTubeUtil
import org.sunbird.spec.BaseTestSpec

class YouTubeUtilSpec extends BaseTestSpec {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig = new AssetEnrichmentConfig(config)

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
    val result = new YouTubeUtil(jobConfig).getVideoInfo("", "snippet,contentDetails", List[String]("thumbnail", "duration", "license"))
    result.isEmpty should be(true)
  }

  "getVideoInfo" should "return the empty data for incorrect Youtube URL" in {
    val result = new YouTubeUtil(jobConfig).getVideoInfo("https://www.youtube.com/watch?v=-SgZ3En23sd", "snippet,contentDetails", List[String]("thumbnail", "duration", "xyz"))
    result.isEmpty should be(true)
  }

  "getVideoInfo" should "throw Exception for null URL" ignore {
    assertThrows[Exception] {
      new YouTubeUtil(jobConfig).getVideoInfo(null, "snippet,contentDetails", List[String]("thumbnail", "duration", "xyz"))
    }
  }

  "getVideo" should " throw exception for null video Id " in {
    assertThrows[Exception] {
      new YouTubeUtil(jobConfig).getVideo(null, null)
    }
  }

}
