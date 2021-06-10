package org.sunbird.job.mvcindexer.domain

import org.apache.commons.lang3.BooleanUtils
import org.sunbird.job.domain.reader.JobRequest

import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.Date

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val df:DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

  private val jobName = "MVCIndexer"

  def index: AnyRef = readOrDefault("index", "")

  def ets: Long = readOrDefault("ets", 0L)

  def userId: String = readOrDefault("userId", "")

  def identifier: String = readOrDefault("object.id", "")

  var eventData: Map[String, AnyRef] = readOrDefault("eventData", new util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

  def action: String = readOrDefault("eventData.action", "")

  def mlContentText:String = readOrDefault("eventData.ml_contentText", null)

  def mlKeywords:List[String] = readOrDefault("eventData.ml_Keywords", null)

  def mlContentTextVector:List[Double] = {
    val mlContentTextVectorList = readOrDefault[List[List[Double]]]("eventData.ml_contentTextVector", null)

    if (mlContentTextVectorList != null) mlContentTextVectorList.head else null
  }

  def audit: Boolean = readOrDefault("audit", true)

  def isValid: Boolean = {
    BooleanUtils.toBoolean(if (null == index) "true" else index.toString)
  }



}
