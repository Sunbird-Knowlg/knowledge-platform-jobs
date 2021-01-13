package org.sunbird.job.service.impl

import org.sunbird.job.util.{AzureRequestBody, AzureResult, HttpRestUtil, MediaRequest, MediaResponse, Response}
import org.sunbird.job.service.AzureMediaService
import org.sunbird.job.task.VideoStreamGeneratorConfig

import scala.collection.immutable.HashMap


object AzureMediaServiceImpl extends AzureMediaService {

  override def submitJob(request: MediaRequest)(implicit config: VideoStreamGeneratorConfig): MediaResponse = {
    println("implicit config ::" + config.getConfig("kafka.input.topic"))
    println("implicit config 2::" + request.request.getOrElse("artifactUrl", "").toString)
    val inputUrl = request.request.getOrElse("artifactUrl", "").toString
    val contentId = request.request.get("identifier").mkString
    val jobId = contentId + "_" + 1
    val temp = inputUrl.splitAt(inputUrl.lastIndexOf("/") + 1)
    val assetId = "asset-" + jobId

    val createAssetResponse = createAsset(assetId, jobId)

    if (createAssetResponse.responseCode.equalsIgnoreCase("OK")) {
      val apiUrl = getApiUrl("job").replace("jobIdentifier", jobId)
      val reqBody = AzureRequestBody.submit_job.replace("assetId", assetId).replace("baseInputUrl", temp._1).replace("inputVideoFile", temp._2)
      val response = HttpRestUtil.put(apiUrl, getDefaultHeader(), reqBody)
      if (response.responseCode == "OK") Response.getSuccessResponse(AzureResult.getSubmitJobResult(response)) else response
    } else {
      Response.getFailureResponse(createAssetResponse.result, "SERVER_ERROR", "Output Asset [ " + assetId + " ] Creation Failed for Job : " + jobId)
    }
  }

  override def getJob(jobId: String)(implicit config: VideoStreamGeneratorConfig): MediaResponse = {
    val response = getJobDetails(jobId)
    if (response.responseCode == "OK") Response.getSuccessResponse(AzureResult.getSubmitJobResult(response)) else response
  }

  override def getStreamingPaths(jobId: String)(implicit config: VideoStreamGeneratorConfig): MediaResponse = {
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
