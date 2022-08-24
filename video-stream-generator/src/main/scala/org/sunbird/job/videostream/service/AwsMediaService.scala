package org.sunbird.job.videostream.service

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.sunbird.job.util.HttpUtil
import org.sunbird.job.videostream.exception.MediaServiceException
import org.sunbird.job.videostream.helpers.{AwsRequestBody, AwsSignUtils, MediaResponse, Response}
import org.sunbird.job.videostream.task.VideoStreamGeneratorConfig

import scala.collection.immutable.HashMap
import scala.reflect.io.File

abstract class AwsMediaService extends IMediaService {

	protected def getApiUrl(apiName: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): String = {
		val host: String = config.getConfig("aws.api.endpoint")
		val apiVersion: String = config.getConfig("aws.api.version")
		val baseUrl: String = host + File.separator + apiVersion
		apiName.toLowerCase() match {
			case "job" => baseUrl + "/jobs"
			case _ => throw new MediaServiceException("ERR_INVALID_API_NAME", "Please Provide Valid AWS Media Service API Name")
		}
	}

	protected def getJobDetails(jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
		val url = getApiUrl("job") + "/" + jobId
		val header = getDefaultHeader("GET", url, null)
		Response.getResponse(httpUtil.get(url, header))
	}

	protected def prepareJobRequestBody(jobRequest: Map[String, AnyRef])(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): String = {
		val queue = config.getConfig("aws.service.queue")
		val role = config.getConfig("aws.service.role")
		val streamType = config.getConfig("aws.stream.protocol").toLowerCase()
		val artifactUrl = jobRequest.get("artifactUrl").mkString
		val contentId = jobRequest.get("identifier").mkString
		val pkgVersion = jobRequest.getOrElse("pkgVersion", "").toString
		val inputFile = prepareInputUrl(artifactUrl)
		val output = prepareOutputUrl(contentId, streamType, pkgVersion)
		AwsRequestBody.submit_hls_job
		  .replace("queueId", queue)
		  .replace("mediaRole", role)
		  .replace("inputVideoFile", inputFile)
		  .replace("outputLocation", output)
	}

	protected def prepareInputUrl(url: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): String = {
		val temp = url.split("content")
		val bucket = config.getConfig("aws.content_bucket_name")
		val separator = File.separator;
		"s3:" + separator + separator + bucket + separator + "content" + temp(1)
	}

	protected def prepareOutputUrl(contentId: String, streamType: String, pkgVersion: String)(implicit config: VideoStreamGeneratorConfig): String = {
		val bucket = config.getConfig("aws.content_bucket_name")
		val output = streamType.toLowerCase + "_" + pkgVersion
		val separator = File.separator;
		"s3:" + separator + separator + bucket + separator + "content" + separator + contentId + separator + output + separator
	}

	protected def getSignatureHeader()(implicit config: VideoStreamGeneratorConfig): Map[String, String] = {
		val formatter = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'")
		formatter.setTimeZone(TimeZone.getTimeZone("UTC"))
		val date = formatter.format(new Date())
		val host: String = config.getConfig("aws.api.endpoint").replace("https://", "")
		Map[String, String]("Content-Type" -> "application/json", "host" -> host, "x-amz-date" -> date)
	}

	protected def getDefaultHeader(httpMethod: String, url: String, payload: String)(implicit config: VideoStreamGeneratorConfig): Map[String, String] = {
		val signHeader = getSignatureHeader
		val authToken = AwsSignUtils.generateToken(httpMethod, url, signHeader, payload)
		val host: String = config.getConfig("aws.api.endpoint").replace("https://", "")
		HashMap[String, String](
			"Content-Type" -> "application/json",
			"host" -> host,
			"x-amz-date" -> signHeader.get("x-amz-date").mkString,
			"Authorization" -> authToken
		)
	}
}
