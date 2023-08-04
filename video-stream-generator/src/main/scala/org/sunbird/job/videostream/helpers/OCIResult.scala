package org.sunbird.job.videostream.helpers

import org.apache.commons.lang3.StringUtils

import scala.collection.immutable.HashMap

object OCIResult extends Result {

  override def getSubmitJobResult(response: MediaResponse): Map[String, AnyRef] = {
    val result = response.result
    HashMap[String, AnyRef](
      "job" -> HashMap[String, AnyRef](
        "id" -> result.getOrElse("id", "").toString,
        "status" -> result.getOrElse("lifecycleState", "").toString.toUpperCase(),
        "submittedOn" -> result.getOrElse("timeCreated", "").toString,
        "lastModifiedOn" -> result.getOrElse("timeUpdated", "").toString
      )
    )
  }

  override def getJobResult(response: MediaResponse): Map[String, AnyRef] = {
    val result = response.result

    HashMap[String, AnyRef](
      "job" -> HashMap[String, AnyRef](
        "id" -> result.getOrElse("id", "").toString,
        "status" -> result.getOrElse("lifecycleState", "").toString.toUpperCase(),
        "submittedOn" -> result.getOrElse("timeCreated", "").toString,
        "lastModifiedOn" -> result.getOrElse("timeUpdated", "").toString,
        "error" -> {
          if (StringUtils.equalsIgnoreCase(result.getOrElse("lifecycleState", "").toString.toUpperCase(),"FAILED")) {
            Map[String, String](
              "errorCode" -> result.getOrElse("lifecycleState", "").toString,
              "errorMessage" -> result.getOrElse("lifecycleDetails", "").toString
            )
          } else {
            null
          }
        }
      )
    )
  }

  override def getCancelJobResult(response: MediaResponse): Map[String, AnyRef] = {
    null
  }

  override def getListJobResult(response: MediaResponse): Map[String, AnyRef] = {
    null
  }

}
