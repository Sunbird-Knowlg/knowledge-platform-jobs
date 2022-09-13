package org.sunbird.job.videostream.helpers


import java.text.SimpleDateFormat
import java.util.{Date, TimeZone, UUID}

import com.google.cloud.video.transcoder.v1.Job

import scala.collection.immutable.HashMap
import org.apache.commons.lang3.StringUtils
import org.sunbird.job.util.{HTTPResponse, JSONUtil}


object Response {

  val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  formatter.setTimeZone(TimeZone.getTimeZone("UTC"))

  def getResponse(response: HTTPResponse): MediaResponse = {
    var result: Map[String, AnyRef] = new HashMap[String, AnyRef]

    try {
      val body = response.body
      if (StringUtils.isNotBlank(body))
        result = JSONUtil.deserialize[Map[String, AnyRef]](body)
    } catch {
      case e: UnsupportedOperationException => e.printStackTrace()
      case e: Exception => e.printStackTrace()
    }

    response.status match {
      case 200 => getSuccessResponse(result)
      case 201 => getSuccessResponse(result)
      case 400 => getFailureResponse(result, "BAD_REQUEST", "Please Provide Correct Request Data.")
      case 401 => getFailureResponse(result, "SERVER_ERROR", "Access Token Expired.")
      case 404 => getFailureResponse(result, "RESOURCE_NOT_FOUND", "Resource Not Found.")
      case 405 => getFailureResponse(result, "METHOD_NOT_ALLOWED", "Requested Operation Not Allowed.")
      case 500 => getFailureResponse(result, "SERVER_ERROR", "Internal Server Error. Please Try Again Later!")
      case _ => getFailureResponse(result, "SERVER_ERROR", "Internal Server Error. Please Try Again Later!")
    }

  }

  def getSuccessResponse(result: Map[String, AnyRef]): MediaResponse = {
    MediaResponse(UUID.randomUUID().toString, System.currentTimeMillis().toString, new HashMap[String, AnyRef],
      ResponseCode.OK.toString, result)
  }

  def getFailureResponse(result: Map[String, AnyRef], errorCode: String, errorMessage: String): MediaResponse = {
    val respCode: String = errorCode match {
      case "BAD_REQUEST" => ResponseCode.CLIENT_ERROR.toString
      case "RESOURCE_NOT_FOUND" => ResponseCode.RESOURCE_NOT_FOUND.toString
      case "METHOD_NOT_ALLOWED" => ResponseCode.CLIENT_ERROR.toString
      case "SERVER_ERROR" => ResponseCode.SERVER_ERROR.toString
    }
    val params = HashMap[String, String](
      "err" -> errorCode,
      "errMsg" -> errorMessage
    )
    MediaResponse(UUID.randomUUID().toString, System.currentTimeMillis().toString, params, respCode, result)
  }

  def getCancelJobResult(response: MediaResponse): Map[String, AnyRef] = {
    null
  }

  def getListJobResult(response: MediaResponse): Map[String, AnyRef] = {
    null
  }

  def getGCPResponse(job: Job): MediaResponse = {
    if (null != job) {
      val jobId = job.getName.split("jobs/")(1)
      val status = job.getState.toString
      val submittedOn = formatter.format(new Date(job.getCreateTime.getSeconds * 1000))
      val result = Map("job" -> Map("id" -> jobId, "status" -> status, "submittedOn" -> submittedOn))
      job.getState.getNumber match {
        case 1 | 2 | 3 => getSuccessResponse(result)
        case 0 | -1 => getFailureResponse(Map("error" -> Map("status" -> status, "errorCode" -> job.getError.getCode.toString, "errorMessage" -> job.getError.getMessage)), "SERVER_ERROR", "Internal Server Error. Please Try Again Later!")
        case 4 => {
          val resultWithError = Map("job" -> Map("id" -> jobId, "status" -> status, "submittedOn" -> submittedOn, "error" -> Map("errorCode" -> job.getError.getCode.toString, "errorMessage" -> job.getError.getMessage)))
          getSuccessResponse(resultWithError)
        }
        case _ => getFailureResponse(Map("error" -> Map("errorCode" -> "SERVER_ERROR", "errorMessage" -> "Unable to get valid response from server.")), "SERVER_ERROR", "Internal Server Error. Please Try Again Later!")
      }
    } else getFailureResponse(Map("error" -> Map("errorCode" -> "SERVER_ERROR", "errorMessage" -> "Unable to get valid response from server.")), "SERVER_ERROR", "Internal Server Error. Please Try Again Later!")
  }


}
