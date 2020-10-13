package org.sunbird.job.Exceptions


case class ValidationException(errorCode: String, msg: String, ex: Exception = null) extends Exception(msg, ex)  {

}
