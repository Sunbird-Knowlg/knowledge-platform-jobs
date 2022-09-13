package org.sunbird.job.videostream.service

import com.google.cloud.video.transcoder.v1.{CreateJobRequest, Job, LocationName}
import org.sunbird.job.videostream.exception.MediaServiceException
import org.sunbird.job.videostream.helpers.GCPAuthUtil
import org.sunbird.job.videostream.task.VideoStreamGeneratorConfig
import com.google.cloud.video.transcoder.v1.GetJobRequest
import com.google.cloud.video.transcoder.v1.JobName
import org.slf4j.LoggerFactory

import scala.reflect.io.File

abstract class GCPMediaService extends IMediaService {

	private[this] val logger = LoggerFactory.getLogger(classOf[GCPMediaService])

	protected def getJobDetails(jobId: String)(implicit config: VideoStreamGeneratorConfig): Job = {
		val transcoderServiceClient = GCPAuthUtil.getTranscoderServiceClient()
		val jobName = JobName.newBuilder.setProject(config.getConfig("gcp.project_id")).setLocation(config.getConfig("gcp.location")).setJob(jobId).build
		val getJobRequest = GetJobRequest.newBuilder.setName(jobName.toString).build
		transcoderServiceClient.getJob(getJobRequest)
	}

	protected def prepareInputUrl(url: String)(implicit config: VideoStreamGeneratorConfig): String = {
		val temp = url.split("/content")
		val bucket = config.getConfig("gcp.content_bucket_name")
		val separator = File.separator
		val ipUrl = "gs:" + separator + separator + bucket + separator + "content" + temp(temp.length-1)
		logger.info("input url generated : "+ipUrl)
		ipUrl
	}

	protected def prepareOutputUrl(contentId: String, streamType: String, pkgVersion: String)(implicit config: VideoStreamGeneratorConfig): String = {
		val bucket = config.getConfig("gcp.content_bucket_name")
		val output = streamType.toLowerCase + "_" + pkgVersion
		val separator = File.separator;
		val opUrl = "gs:" + separator + separator + bucket + separator + "content" + separator + contentId + separator + output + separator
		logger.info("output url generated : "+opUrl)
		opUrl
	}

	protected def createJobFromTemplate(inputUri: String, outputUri: String, template: String)(implicit config: VideoStreamGeneratorConfig): Job = {
		try {
			val transcoderServiceClient = GCPAuthUtil.getTranscoderServiceClient()
			val createJobRequest = CreateJobRequest.newBuilder.setJob(
				Job.newBuilder.setInputUri(inputUri).setOutputUri(outputUri).setTemplateId(template).build)
			  .setParent(LocationName.of(config.getConfig("gcp.project_id"), config.getConfig("gcp.location")).toString).build
			transcoderServiceClient.createJob(createJobRequest)
		} catch {
			case ex: Exception => {
				ex.printStackTrace()
				throw new MediaServiceException("ERR_GCP_CREATE_JOB_TEMPLATE", s"Unable to create job using template: ${template}. Exception is: " + ex.getMessage)
			}
		}
	}

}
