package org.sunbird.job.videostream.service.impl

import org.sunbird.job.util.HttpUtil
import org.sunbird.job.videostream.helpers.{AwsResult, MediaRequest, MediaResponse, Response}
import org.sunbird.job.videostream.service.AwsMediaService
import org.sunbird.job.videostream.task.VideoStreamGeneratorConfig

import scala.collection.immutable.HashMap

object AwsMediaServiceImpl extends AwsMediaService {

	override def submitJob(request: MediaRequest)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
		val url = getApiUrl("job")
		val reqBody = prepareJobRequestBody(request.request)
		val header = getDefaultHeader("POST", url, reqBody)
		val response:MediaResponse = Response.getResponse(httpUtil.post(url, reqBody, header))
		if (response.responseCode == "OK") Response.getSuccessResponse(AwsResult.getSubmitJobResult(response)) else response
	}

	override def getJob(jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
		val response = getJobDetails(jobId)
		if (response.responseCode == "OK") Response.getSuccessResponse(AwsResult.getJobResult(response)) else response
	}

	override def getStreamingPaths(jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
		val region = config.getConfig("aws.region");
		val streamType = config.getConfig("aws.stream.protocol").toLowerCase()
		val getResponse = getJobDetails(jobId)
		val inputs: List[Map[String, AnyRef]] = getResponse.result.getOrElse("job", Map).asInstanceOf[Map[String, AnyRef]].getOrElse("settings", Map).asInstanceOf[Map[String, AnyRef]].getOrElse("inputs", List).asInstanceOf[List[Map[String, AnyRef]]]
		val input: String = inputs.head.getOrElse("fileInput", "").toString
		val host = "https://s3." + region + ".amazonaws.com"
		val streamUrl: String = input.replace("s3:/", host)
		  .replace("artifact", streamType)
		  .replace(".mp4", ".m3u8")
  		  .replace(".webm", ".m3u8")
		Response.getSuccessResponse(HashMap[String, AnyRef]("streamUrl" -> streamUrl))
	}

	override def listJobs(listJobsRequest: MediaRequest): MediaResponse = ???

	override def cancelJob(cancelJobRequest: MediaRequest): MediaResponse = ???
}
