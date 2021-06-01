package org.sunbird.job.videostream.exception

class MediaServiceException(var errorCode: String = null, msg: String, ex: Exception = null) extends Exception(msg, ex)
