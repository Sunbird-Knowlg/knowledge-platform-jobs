package org.sunbird.job.videostream.service

import org.sunbird.job.videostream.task.VideoStreamGeneratorConfig
import org.sunbird.job.util.HttpUtil
import org.sunbird.job.videostream.helpers.{MediaRequest, MediaResponse}


trait IMediaService {

  def submitJob(request: MediaRequest)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse

  def getJob(jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse

  def getStreamingPaths(jobId: String)(implicit config: VideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse

  def listJobs(listJobsRequest: MediaRequest): MediaResponse

  def cancelJob(cancelJobRequest: MediaRequest): MediaResponse

}
