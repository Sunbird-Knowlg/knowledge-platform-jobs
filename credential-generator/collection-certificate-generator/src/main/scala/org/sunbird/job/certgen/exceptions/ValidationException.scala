package org.sunbird.job.certgen.exceptions

case class ValidationException(errorCode: String, msg: String, ex: Exception = null) extends Exception(msg, ex)  {

}

case class ServerException (errorCode: String, msg: String, ex: Exception = null) extends Exception(msg, ex) {
}
