package org.sunbird.job.livevideostream.service.impl

import org.sunbird.job.livevideostream.exception.MediaServiceException
import org.sunbird.job.livevideostream.service.IMediaService
import org.sunbird.job.livevideostream.task.LiveVideoStreamGeneratorConfig

object MediaServiceFactory {

  def getMediaService(config: LiveVideoStreamGeneratorConfig): IMediaService = {
    val serviceType: String = config.getConfig("media_service_type")
    serviceType match {
      case "azure" => AzureMediaServiceImpl
      case "aws" => AwsMediaServiceImpl
      case _ => throw new MediaServiceException("ERR_INVALID_SERVICE_TYPE", "Please Provide Valid Media Service Name")
    }
  }
}
