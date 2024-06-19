package org.sunbird.job.videostream.service

import java.io.File
import org.apache.commons.lang3.StringUtils
import org.sunbird.job.videostream.exception.MediaServiceException
import org.sunbird.job.util.{HttpUtil, JSONUtil}
import org.sunbird.job.videostream.helpers.{AzureRequestBody, MediaResponse, Response}
import org.sunbird.job.videostream.task.VideoStreamGeneratorConfig

import scala.collection.immutable.HashMap


abstract class AzureMediaService extends IMediaService {

  private var API_ACCESS_TOKEN: String = ""
  
  protected def getJobDetails(jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
    val url = getApiUrl("job").replace("jobIdentifier", jobId)
    val response:MediaResponse = Response.getResponse(httpUtil.get(url, getDefaultHeader()))
    if(response.responseCode == "OK"){
      response
    } else {
      throw new Exception("Error while getting the job detail::"+JSONUtil.serialize(response))
    }
  }

  protected def createAsset(assetId: String, jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
    val accountName: String = config.getConfig("azure_mediakind.account_name")
    val url = getApiUrl("asset").replace("assetId", assetId)
    val requestBody = AzureRequestBody.create_asset.replace("assetId", assetId)
      .replace("assetDescription", "Output Asset for " + jobId)
      .replace("assetStorageAccountName", accountName)
    val response:MediaResponse = Response.getResponse(httpUtil.put(url, requestBody, getDefaultHeader()))
    if(response.responseCode == "OK"){
      response
    } else {
      throw new Exception("Error while creating asset::(assetId->"+assetId+", jobId->"+jobId+")::"+JSONUtil.serialize(response))
    }
  }

  protected def createStreamingLocator(streamingLocatorName: String, assetName: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
    val url = getApiUrl("stream_locator").replace("streamingLocatorName", streamingLocatorName)
    val streamingPolicyName = config.getConfig("azure_mediakind.stream.policy_name")
    val reqBody = AzureRequestBody.create_stream_locator.replace("assetId", assetName).replace("policyName", streamingPolicyName)
    Response.getResponse(httpUtil.put(url, reqBody, getDefaultHeader()))
  }

  protected def getStreamingLocator(streamingLocatorName: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
    val url = getApiUrl("stream_locator").replace("streamingLocatorName", streamingLocatorName)
    val response:MediaResponse = Response.getResponse(httpUtil.get(url, getDefaultHeader()))
    if(response.responseCode == "OK"){
      response
    } else {
      throw new Exception("Error while getStreamingLocator::(streamingLocatorName->" + streamingLocatorName + ")::"+JSONUtil.serialize(response))
    }
  }

  protected def getStreamUrls(streamingLocatorName: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
    val url = getApiUrl("list_paths").replace("streamingLocatorName", streamingLocatorName)
    val response:MediaResponse = Response.getResponse(httpUtil.post(url, "{}", getDefaultHeader()))
    if(response.responseCode == "OK"){
      response
    } else {
      throw new Exception("Error while getStreamUrls::(streamingLocatorName->" + streamingLocatorName + ")::"+JSONUtil.serialize(response))
    }
  }

  protected def getApiUrl(apiName: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): String = {
    val transformName: String = config.getConfig("azure_mediakind.transform.default")
    val projectName:String = config.getConfig("azure_mediakind.project_name")

    val baseUrl: String = new StringBuilder().append(config.getConfig("azure_mediakind.api.endpoint")+"/ams/")
      .append(projectName).mkString


    apiName.toLowerCase() match {
      case "asset" => baseUrl + "/assets/assetId"
      case "job" => baseUrl + "/transforms/" + transformName + "/jobs/jobIdentifier"
      case "stream_locator" => baseUrl + "/streamingLocators/streamingLocatorName"
      case "list_paths" => baseUrl + "/streamingLocators/streamingLocatorName/listPaths"
      case _ => throw new MediaServiceException("ERR_INVALID_API_NAME", "Please Provide Valid Media Service API Name")
    }
  }

  protected def getDefaultHeader()(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): Map[String, String] = {
    val authToken = config.getConfig("azure_mediakind.auth_token")
    HashMap[String, String](
      "content-type" -> "application/json",
      "accept" -> "application/json",
      "x-mkio-token" -> authToken
    )
  }

  protected def prepareStreamingUrl(streamLocatorName: String, jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): Map[String, AnyRef] = {
    val streamType = config.getConfig("azure_mediakind.stream.protocol")
    val streamHost = config.getConfig("azure_mediakind.stream.base_url")
    var url = ""
    val listPathResponse = getStreamUrls(streamLocatorName)
    if (listPathResponse.responseCode.equalsIgnoreCase("OK")) {
      val urlList: List[Map[String, AnyRef]] = listPathResponse.result.getOrElse("streamingPaths", List).asInstanceOf[List[Map[String, AnyRef]]]
      urlList.foreach(streamMap => {
        if (StringUtils.equalsIgnoreCase(streamMap.getOrElse("streamingProtocol", null).toString, streamType)) {
          url = streamMap("paths").asInstanceOf[List[String]].head
        }
      })
      val streamUrl = streamHost + url.replace("aapl", "aapl-v3")
      HashMap[String, AnyRef]("streamUrl" -> streamUrl)
    } else {
      val getResponse: MediaResponse = getJobDetails(jobId)
      val fileName: String = getResponse.result.getOrElse("properties", Map).asInstanceOf[Map[String, AnyRef]].getOrElse("input", Map).asInstanceOf[Map[String, AnyRef]].getOrElse("files", List).asInstanceOf[List[AnyRef]].head.toString
      val getStreamResponse = getStreamingLocator(streamLocatorName);
      val locatorId = getStreamResponse.result.getOrElse("properties", Map).asInstanceOf[Map[String, AnyRef]].getOrElse("streamingLocatorId", "").toString
      val streamUrl = streamHost + File.separator + locatorId + File.separator + fileName.replace(".mp4", ".ism") + "/manifest(format=m3u8-aapl-v3)"
      HashMap[String, AnyRef]("streamUrl" -> streamUrl)
    }
  }
}