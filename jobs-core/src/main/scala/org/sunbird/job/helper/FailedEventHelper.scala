package org.sunbird.job.helper

import org.sunbird.job.util.ScalaJsonUtil

import java.io.{PrintWriter, StringWriter}

trait FailedEventHelper {

  def getFailedEvent(jobName: String, eventMap: java.util.Map[String, Any], error: Throwable): String = {
    val errorString = getStackTrace(error).split("\\n\\t")
    val stackTrace = if (errorString.length > 21) errorString.toList.slice(errorString.length - 21, errorString.length - 1) else errorString.toList
    getFailedEvent(jobName, eventMap, s"${error.getMessage} : : $stackTrace")
  }

  def getFailedEvent(jobName: String, eventMap: java.util.Map[String, Any], errorString: String): String = {
    val failedEventMap = Map("error" -> s"$errorString")
    eventMap.put("jobName", jobName)
    eventMap.put("failInfo", failedEventMap)
    ScalaJsonUtil.serialize(eventMap)
  }

  def getStackTrace(throwable: Throwable): String = {
    val sw = new StringWriter
    val pw = new PrintWriter(sw, true)
    throwable.printStackTrace(pw)
    sw.getBuffer.toString
  }
}
