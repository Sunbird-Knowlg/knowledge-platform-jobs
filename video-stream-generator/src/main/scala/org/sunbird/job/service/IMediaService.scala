package org.sunbird.job.service

import org.sunbird.job.task.VideoStreamGeneratorConfig
import org.sunbird.job.util.{MediaRequest, MediaResponse}


trait IMediaService {

  def submitJob(request: MediaRequest)(implicit config: VideoStreamGeneratorConfig): MediaResponse

  def getJob(jobId: String)(implicit config: VideoStreamGeneratorConfig): MediaResponse

  def getStreamingPaths(jobId: String)(implicit config: VideoStreamGeneratorConfig): MediaResponse

  def listJobs(listJobsRequest: MediaRequest): MediaResponse

  def cancelJob(cancelJobRequest: MediaRequest): MediaResponse

}
