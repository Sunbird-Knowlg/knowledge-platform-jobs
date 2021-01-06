package org.sunbird.job.Exceptions

object ErrorMessages {

  val INVALID_REQUESTED_DATA: String = "Invalid Request! Please Provide Valid Request."
  val INVALID_PARAM_VALUE: String = "Invalid value {0} for parameter {1}."
  val MANDATORY_PARAMETER_MISSING: String = "Mandatory parameter {0} is missing."
}

object ErrorCodes {
  val MANDATORY_PARAMETER_MISSING: String = "MANDATORY_PARAMETER_MISSING"
  val INVALID_PARAM_VALUE: String = "INVALID_PARAM_VALUE"
  val SYSTEM_ERROR: String = "SYSTEM_ERROR"
}


