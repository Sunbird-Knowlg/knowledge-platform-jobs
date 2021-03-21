package org.sunbird.job.publish.domain

import java.util

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

	private val jobName = "content-publish"

	private val objectTypes = List("Content", "ContentImage")
	//private val mimeTypes = List("application/vnd.sunbird.question", "application/vnd.sunbird.questionset")

	def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asScala.toMap

	def objectData: Map[String, AnyRef] =  readOrDefault("object", new util.HashMap[String, AnyRef]()).asScala.toMap

	def action: String = readOrDefault[String]("edata.action", "")

	def mimeType: String = readOrDefault[String]("edata.metadata.mimeType", "")

	def objectId: String = readOrDefault[String]("object.id", "")

	def objectType: String = readOrDefault[String]("object.type", "")

	def iteration: Number = readOrDefault[Number]("edata.iteration", 1)

	def lastPublishedBy: String = readOrDefault[String]("edata.metadata.lastPublishedBy", "")

	def publishType: String = readOrDefault[String]("edata.publish_type", "")
  
	def pkgVersion: Double = {
    val pkgVersion = readOrDefault[Int]("edata.metadata.pkgVersion", 0)
    pkgVersion.toDouble
  }

	def validEvent(): Boolean = {
		//TODO: Apply iteration<=maxIterationCount condition
		(StringUtils.equals("publish", action) && StringUtils.isNotBlank(objectId) && StringUtils.isNotBlank(publishType) && (objectTypes.contains(objectType)))
	}
}