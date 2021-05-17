package org.sunbird.job.assetenricment.domain

import java.util
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  val jobName = "AssetEnrichment"

  val data: util.Map[String, Any] = getMap()

  def eData: Map[String, AnyRef] = eventMap.getOrDefault("edata", new java.util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

  def objectData: Map[String, AnyRef] = eventMap.getOrDefault("object", new java.util.HashMap[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]

  def id: String = objectData.getOrElse("id", "").asInstanceOf[String]

  def objectType: String = eData.getOrElse("objectType", "").asInstanceOf[String]

  def mediaType: String = eData.getOrElse("mediaType", "").asInstanceOf[String]

  def status: String = eData.getOrElse("status", "").asInstanceOf[String]

  def validate(maxIterationCount: Int): String = {
    val iteration = eData.getOrElse("iteration", 0).asInstanceOf[Int]
    if (id.isEmpty) s"Invalid ID present in the Event for the Object Data: ${objectData}."
    else if (!objectType.equalsIgnoreCase("asset")) s"Ignoring Event due to ObjectType : ${objectType} for ID : ${id}."
    else if (!mediaType.equalsIgnoreCase("image") && !mediaType.equalsIgnoreCase("video")) s"Ignoring Event due to MediaType: ${mediaType} for ID : ${id}."
    else if (iteration == 1 && status.equalsIgnoreCase("processing")) ""
    else if (iteration > 1 && iteration <= maxIterationCount && status.equalsIgnoreCase("failed")) ""
    else s"Ignoring Event due to Iteration Limit Exceed. Iteration Count : ${iteration} for ID : ${id}."
  }
}
