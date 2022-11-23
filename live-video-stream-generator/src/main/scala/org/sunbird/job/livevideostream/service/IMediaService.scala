package org.sunbird.job.livevideostream.service

import org.sunbird.job.livevideostream.task.LiveVideoStreamGeneratorConfig
import org.sunbird.job.util.HttpUtil
import org.sunbird.job.livevideostream.helpers.{MediaRequest, MediaResponse}


trait IMediaService {

  def submitJob(request: MediaRequest)(implicit config: LiveVideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse

  def getJob(jobId: String)(implicit config: LiveVideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse

  def getStreamingPaths(jobId: String)(implicit config: LiveVideoStreamGeneratorConfig, httpUtil: HttpUtil): MediaResponse

  def listJobs(listJobsRequest: MediaRequest): MediaResponse

  def cancelJob(cancelJobRequest: MediaRequest): MediaResponse

}
