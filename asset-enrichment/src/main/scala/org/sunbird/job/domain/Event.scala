package org.sunbird.job.domain

import java.util

import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

  val jobName = "AssetEnrichment"

  val data: util.Map[String, Any] = getMap()

  def eData: Map[String, AnyRef] = eventMap.getOrDefault("edata", new java.util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

  def objectData: Map[String, AnyRef] = eventMap.getOrDefault("object", new java.util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

  def id: String = objectData.getOrElse("id", "").asInstanceOf[String]

  def objectType: String = eData.getOrElse("objectType", "").asInstanceOf[String]

  def mediaType: String = eData.getOrElse("mediaType", "").asInstanceOf[String]

  def status: String = eData.getOrElse("status", "").asInstanceOf[String]

}
