package org.sunbird.job.service

import java.io.File

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.exception.MediaServiceException
import org.sunbird.job.task.VideoStreamGeneratorConfig
import org.sunbird.job.util.{AzureRequestBody, HttpRestUtil, MediaResponse}

import scala.collection.immutable.HashMap


abstract class AzureMediaService extends IMediaService {

  private var API_ACCESS_TOKEN: String = ""

  private def getToken()(implicit config: VideoStreamGeneratorConfig): String = {
    val tenant = config.getSystemConfig("azure.tenant")
    val clientKey = config.getSystemConfig("azure.token.client_key")
    val clientSecret = config.getSystemConfig("azure.token.client_secret")
    val loginUrl = config.getConfig("azure.login.endpoint") + "/" + tenant + "/oauth2/token"

    val data = HashMap[String, String](
      "grant_type" -> "client_credentials",
      "client_id" -> clientKey,
      "client_secret" -> clientSecret,
      "resource" -> "https://management.core.windows.net/"
    )

    val header = HashMap[String, String](
      "Content-Type" -> "application/x-www-form-urlencoded",
      "Keep-Alive" -> "true"
    )

    val response = HttpRestUtil.post(loginUrl, header, data)
    response.result.getOrElse("access_token", "").toString
  }

  protected def getJobDetails(jobId: String)(implicit config: VideoStreamGeneratorConfig): MediaResponse = {
    val url = getApiUrl("job").replace("jobIdentifier", jobId)
    HttpRestUtil.get(url, getDefaultHeader(), null)
  }

  protected def createAsset(assetId: String, jobId: String)(implicit config: VideoStreamGeneratorConfig): MediaResponse = {
    val url = getApiUrl("asset").replace("assetId", assetId)
    val requestBody = AzureRequestBody.create_asset.replace("assetId", assetId)
      .replace("assetDescription", "Output Asset for " + jobId)
    HttpRestUtil.put(url, getDefaultHeader(), requestBody)
  }

  protected def createStreamingLocator(streamingLocatorName: String, assetName: String)(implicit config: VideoStreamGeneratorConfig): MediaResponse = {
    val url = getApiUrl("stream_locator").replace("streamingLocatorName", streamingLocatorName)
    val streamingPolicyName = config.getConfig("azure.stream.policy_name")
    val reqBody = AzureRequestBody.create_stream_locator.replace("assetId", assetName).replace("policyName", streamingPolicyName)
    HttpRestUtil.put(url, getDefaultHeader(), reqBody)
  }

  protected def getStreamingLocator(streamingLocatorName: String)(implicit config: VideoStreamGeneratorConfig): MediaResponse = {
    val url = getApiUrl("stream_locator").replace("streamingLocatorName", streamingLocatorName)
    HttpRestUtil.get(url, getDefaultHeader(), null)
  }

  protected def getStreamUrls(streamingLocatorName: String)(implicit config: VideoStreamGeneratorConfig): MediaResponse = {
    val url = getApiUrl("list_paths").replace("streamingLocatorName", streamingLocatorName)
    HttpRestUtil.post(url, getDefaultHeader(), "{}")
  }

  protected def getApiUrl(apiName: String)(implicit config: VideoStreamGeneratorConfig): String = {
    val subscriptionId: String = config.getSystemConfig("azure.subscription_id")
    val resourceGroupName: String = config.getSystemConfig("azure.resource_group_name")
    val accountName: String = config.getSystemConfig("azure.account_name")
    val apiVersion: String = config.getConfig("azure.api.version")
    val transformName: String = config.getConfig("azure.transform.default")

    val baseUrl: String = new StringBuilder().append(config.getConfig("azure.api.endpoint")+"/subscriptions/")
      .append(subscriptionId)
      .append("/resourceGroups/")
      .append(resourceGroupName)
      .append("/providers/Microsoft.Media/mediaServices/")
      .append(accountName).mkString


    apiName.toLowerCase() match {
      case "asset" => baseUrl + "/assets/assetId?api-version=" + apiVersion
      case "job" => baseUrl + "/transforms/" + transformName + "/jobs/jobIdentifier?api-version=" + apiVersion
      case "stream_locator" => baseUrl + "/streamingLocators/streamingLocatorName?api-version=" + apiVersion
      case "list_paths" => baseUrl + "/streamingLocators/streamingLocatorName/listPaths?api-version=" + apiVersion
      case _ => throw new MediaServiceException("ERR_INVALID_API_NAME", "Please Provide Valid Media Service API Name")
    }
  }

  protected def getDefaultHeader()(implicit config: VideoStreamGeneratorConfig): Map[String, String] = {
    val accessToken = if (StringUtils.isNotBlank(API_ACCESS_TOKEN)) API_ACCESS_TOKEN else getToken()
    val authToken = "Bearer " + accessToken
    HashMap[String, String](
      "Content-Type" -> "application/json",
      "Accept" -> "application/json",
      "Authorization" -> authToken
    )
  }

  protected def prepareStreamingUrl(streamLocatorName: String, jobId: String)(implicit config: VideoStreamGeneratorConfig): Map[String, AnyRef] = {
    val streamType = config.getConfig("azure.stream.protocol")
    val streamHost = config.getConfig("azure.stream.base_url")
    var url = ""
    val listPathResponse = getStreamUrls(streamLocatorName)
    if (listPathResponse.responseCode.equalsIgnoreCase("OK")) {
      val urlList: List[Map[String, AnyRef]] = listPathResponse.result.getOrElse("streamingPaths", List).asInstanceOf[List[Map[String, AnyRef]]]
      urlList.map(streamMap => {
        if (StringUtils.equalsIgnoreCase(streamMap.getOrElse("streamingProtocol", null).toString, streamType)) {
          url = streamMap.get("paths").get.asInstanceOf[List[String]].head
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
