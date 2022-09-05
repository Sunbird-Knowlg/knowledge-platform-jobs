package org.sunbird.job.videostream.service.impl

import io.grpc.StatusRuntimeException
import org.sunbird.job.util.HttpUtil
import org.sunbird.job.videostream.exception.MediaServiceException
import org.sunbird.job.videostream.helpers.{MediaRequest, MediaResponse, Response}
import org.sunbird.job.videostream.service.GCPMediaService
import org.sunbird.job.videostream.task.VideoStreamGeneratorConfig

import scala.collection.immutable.HashMap

object GCPMediaServiceImpl extends GCPMediaService {

	override def submitJob(request: MediaRequest)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
		val identifier = request.request.get("identifier").mkString
		val pkgVersion = request.request.getOrElse("pkgVersion", "").toString
		val streamType = config.getConfig("gcp.stream.protocol").toLowerCase
		val template = config.getConfig("gcp.stream.template_id").toLowerCase
		val inputUri = prepareInputUrl(request.request.get("artifactUrl").mkString)
		val outputUri = prepareOutputUrl(identifier, streamType, pkgVersion)
		try {
			val job = createJobFromTemplate(inputUri, outputUri, template)
			Response.getGCPResponse(job)
		} catch {
			case e: MediaServiceException => Response.getFailureResponse(Map("error" -> Map("errorCode"->e.errorCode, "errorMessage"->e.getMessage)), "ERR_GCP_SUBMIT_JOB", s"Error Occurred While Submitting Job for ${identifier}")
		}
	}

	override def getJob(jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
		try {
			val job = getJobDetails(jobId)
			if (null != job) Response.getGCPResponse(job) else Response.getFailureResponse(Map(), "SERVER_ERROR", s"""Unable to fetch job details with identifier: ${jobId}""")
		} catch {
			case e: StatusRuntimeException => Response.getFailureResponse(Map("error" -> Map("errorCode"->"SERVER_ERROR", "errorMessage"->e.getMessage)), "RESOURCE_NOT_FOUND", s"Resource Not Found for Job Id: ${jobId}.")
			case ex: Exception => throw new MediaServiceException("ERR_GCP_GET_JOB", s"""Unable to fetch job details having jobId: ${jobId} | Exception is: ${ex.getMessage}""")
		}
	}

	override def getStreamingPaths(jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse = {
		try {
			val job = getJobDetails(jobId)
			if (null != job && job.getState.getNumber == 3) {
				val outputUrl = job.getConfig.getOutput.getUri
				val streamUrl = outputUrl.replace("gs://", "https://storage.googleapis.com") + "manifest.m3u8"
				Response.getSuccessResponse(HashMap[String, AnyRef]("streamUrl" -> streamUrl))
			} else Response.getGCPResponse(job)
		} catch {
			case ex: Exception => Response.getFailureResponse(Map("error" -> Map("errorCode"->"SERVER_ERROR", "errorMessage"->ex.getMessage)), "SERVER_ERROR", s"""Unable to fetch job details with identifier: ${jobId}""")
		}
	}

	override def listJobs(listJobsRequest: MediaRequest): MediaResponse = ???

	override def cancelJob(cancelJobRequest: MediaRequest): MediaResponse = ???
}
