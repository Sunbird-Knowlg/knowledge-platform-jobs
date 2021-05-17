package org.sunbird.job.videostream.service.impl

import org.sunbird.job.videostream.exception.MediaServiceException
import org.sunbird.job.videostream.service.IMediaService

//import org.ekstep.media.config.AppConfig


object MediaServiceFactory {

//  val SERVICE_TYPE: String = AppConfig.getServiceType()

  def getMediaService(): IMediaService = {
//    SERVICE_TYPE.toLowerCase() match {
//      case "aws" => AWSMediaServiceImpl
    "azure" match {
      case "azure" => AzureMediaServiceImpl
      case _ => throw new MediaServiceException("ERR_INVALID_SERVICE_TYPE", "Please Provide Valid Media Service Name")
    }
  }
}
