package org.sunbird.job.assetenricment.util

import java.util
import java.util.regex.{Matcher, Pattern}

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.http.{HttpRequest, HttpRequestInitializer}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.youtube.YouTube
import com.google.api.services.youtube.model.Video
import org.slf4j.LoggerFactory
import org.sunbird.job.task.AssetEnrichmentConfig

import scala.collection.JavaConverters._

class YouTubeUtil(config: AssetEnrichmentConfig) {

  private[this] val logger = LoggerFactory.getLogger(classOf[YouTubeUtil])
  private val APP_NAME = config.youtubeAppName
  private val API_KEY = config.getString("content_youtube_apikey", "")
  private val videoIdRegex = config.videoIdRegex.asScala.toList
  private var limitExceeded = false
  private val youtube: YouTube = initializeYoutube()

  def initializeYoutube(): YouTube = {
    new YouTube.Builder(new NetHttpTransport, new JacksonFactory, new HttpRequestInitializer() {
      override def initialize(request: HttpRequest): Unit = {
      }
    }).setApplicationName(APP_NAME).build()
  }

  def getVideoInfo(videoUrl: String, apiParams: String, metadata: List[String]): Map[String, AnyRef] = {
    val videoId = getIdFromUrl(videoUrl)
    videoId match {
      case Some(id: String) =>
        val video: Video = getVideo(id, apiParams)
        if (null != video) {
          metadata.flatMap(str => {
            val res = getResult(str, video)
            if (res.nonEmpty) Map[String, AnyRef](str -> res) else Map[String, AnyRef]()
          }).toMap[String, AnyRef]
        } else Map[String, AnyRef]()
      case _ =>
        logger.error(s"Failed to get Id from the VideoUrl : ${videoUrl}")
        Map[String, AnyRef]()
    }
  }

  def getIdFromUrl(url: String): Option[String] = {
    val videoLink = getVideoLink(url)
    val result = videoIdRegex.map(p => Pattern.compile(p))
      .map(compiledPattern => compiledPattern.matcher(videoLink))
      .find(f => f.find())
      .map((matcher: Matcher) => matcher.group(1)).orElse(null)
    result
  }

  private def getVideoLink(url: String): String = {
    val youTubeUrlRegEx = "^(https?)?(://)?(www.)?(m.)?((youtube.com)|(youtu.be))/"
    val compiledPattern = Pattern.compile(youTubeUrlRegEx)
    val matcher = compiledPattern.matcher(url)
    if (matcher.find) url.replace(matcher.group, "") else url
  }

  def getVideo(videoId: String, params: String): Video = {
    if (limitExceeded) throw new Exception(s"Unable to Check License for videoId = ${videoId}. Please Try Again After Sometime!")
    try {
      val videosListByIdRequest = youtube.videos.list(params)
      videosListByIdRequest.setKey(API_KEY)
      videosListByIdRequest.setId(videoId)
      val response = videosListByIdRequest.execute
      val items = response.getItems
      if (items == null || items.isEmpty) null else items.get(0)
    } catch {
      case ex: GoogleJsonResponseException =>
        val error = ex.getDetails.getErrors.get(0)
        val reason = error.get("reason").asInstanceOf[String]
        limitExceeded = true
        logger.error(s"Youtube API Limit Exceeded while processing for videoId = ${videoId}. Reason is:  ${reason}", ex)
        throw new Exception(s"Unable to Check License for videoId = ${videoId}. Please Try Again After Sometime!")
      case e: Exception =>
        logger.error("Error Occurred While Calling Youtube API. Error Details : ", e)
        throw e
    }
  }

  private def getResult(str: String, video: Video): String = {
    str.toLowerCase match {
      case "license" =>
        video.getStatus.getLicense
      case "thumbnail" =>
        video.getSnippet.getThumbnails.getMedium.getUrl
      case "duration" =>
        computeVideoDuration(video.getContentDetails.getDuration)
      case _ => ""
    }
  }

  def computeVideoDuration(videoDuration: String): String = {
    val youtubeDuration = videoDuration.replaceAll("PT|S", "").replaceAll("H|M", ":")
    val values = youtubeDuration.split(":")
    values.length match {
      case 1 => values(0)
      case 2 => String.valueOf((values(0).toInt * 60) + (values(1).toInt * 1))
      case 3 => String.valueOf((values(0).toInt * 3600) + (values(1).toInt * 60) + (values(2).toInt * 1))
      case _ => ""
    }
  }

}
