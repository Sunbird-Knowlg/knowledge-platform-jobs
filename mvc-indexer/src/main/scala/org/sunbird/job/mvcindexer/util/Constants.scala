package org.sunbird.job.mvcindexer.util

object PlatformErrorCodes extends Enumeration {
  type PlatformErrorCodes = Value
  val ERR_DEFINITION_NOT_FOUND, ERR_HOST_UNAVAILABLE, SYSTEM_ERROR, DATA_ERROR, PROCESSING_ERROR, PUBLISH_FAILED = Value
}