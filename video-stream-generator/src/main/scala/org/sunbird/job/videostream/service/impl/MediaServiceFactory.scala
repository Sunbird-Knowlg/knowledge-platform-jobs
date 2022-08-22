package org.sunbird.job.videostream.service.impl

import org.sunbird.job.videostream.exception.MediaServiceException
import org.sunbird.job.videostream.service.IMediaService
import org.sunbird.job.videostream.task.VideoStreamGeneratorConfig

object MediaServiceFactory {

  def getMediaService(config: VideoStreamGeneratorConfig): IMediaService = {
    val serviceType: String = config.getConfig("media_service_type")
    serviceType match {
      case "azure" => AzureMediaServiceImpl
      case "aws" => AwsMediaServiceImpl
      case _ => throw new MediaServiceException("ERR_INVALID_SERVICE_TYPE", "Please Provide Valid Media Service Name")
    }
  }
}
