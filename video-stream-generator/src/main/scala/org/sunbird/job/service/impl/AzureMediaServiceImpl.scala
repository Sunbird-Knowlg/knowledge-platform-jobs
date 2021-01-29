package org.sunbird.job.service.impl

import org.sunbird.job.util.HttpUtil
import org.sunbird.job.helpers.{AzureRequestBody, AzureResult, MediaRequest, MediaResponse, Response}
import org.sunbird.job.service.AzureMediaService
import org.sunbird.job.task.VideoStreamGeneratorConfig

import scala.collection.immutable.HashMap


object AzureMediaServiceImpl extends AzureMediaService {

  override def submitJob(request: MediaRequest)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
    val inputUrl = request.request.getOrElse("artifactUrl", "").toString
    val contentId = request.request.get("identifier").mkString
    val jobId = contentId + "_" + System.currentTimeMillis()
    val temp = inputUrl.splitAt(inputUrl.lastIndexOf("/") + 1)
    val assetId = "asset-" + jobId

    val createAssetResponse = createAsset(assetId, jobId)

    if (createAssetResponse.responseCode.equalsIgnoreCase("OK")) {
      val apiUrl = getApiUrl("job").replace("jobIdentifier", jobId)
      val reqBody = AzureRequestBody.submit_job.replace("assetId", assetId).replace("baseInputUrl", temp._1).replace("inputVideoFile", temp._2)
      val response:MediaResponse = Response.getResponse(httpUtil.put(apiUrl, reqBody, getDefaultHeader()))
      if (response.responseCode == "OK") Response.getSuccessResponse(AzureResult.getSubmitJobResult(response)) else response
    } else {
      Response.getFailureResponse(createAssetResponse.result, "SERVER_ERROR", "Output Asset [ " + assetId + " ] Creation Failed for Job : " + jobId)
    }
  }

  override def getJob(jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
    val response = getJobDetails(jobId)
    if (response.responseCode == "OK") Response.getSuccessResponse(AzureResult.getSubmitJobResult(response)) else response
  }

  override def getStreamingPaths(jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
    val streamLocatorName = "sl-" + jobId
    val assetName = "asset-" + jobId
    val locatorResponse = createStreamingLocator(streamLocatorName, assetName)
    if (locatorResponse.responseCode == "OK" || locatorResponse.responseCode == "CLIENT_ERROR") {
      Response.getSuccessResponse(prepareStreamingUrl(streamLocatorName, jobId))
    } else {
      Response.getFailureResponse(new HashMap[String, AnyRef], "SERVER_ERROR", "Streaming Locator [" + streamLocatorName + "] Creation Failed for Job : " + jobId)
    }
  }

  override def listJobs(listJobsRequest: MediaRequest): MediaResponse = {
    null
  }

  override def cancelJob(cancelJobRequest: MediaRequest): MediaResponse = {
    null
  }

}
