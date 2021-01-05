package org.sunbird.job.publish.domain

import java.{lang, util}
import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

	private val jobName = "questionset-publish"

	private val objectTypes = List("Question", "QuestionSet")
	private val mimeTypes = List("application/vnd.sunbird.question", "application/vnd.sunbird.questionset")

	def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asScala.toMap

	def action: String = readOrDefault[String]("edata.action", "")

	def mimeType: String = readOrDefault[String]("edata.metadata.mimeType", "")

	def objectId: String = readOrDefault[String]("edata.metadata.identifier", "")

	def objectType: String = readOrDefault[String]("edata.metadata.objectType", "")

  // TODO: revert the code to read as Double after event correction.
	def pkgVersion: Double = {
    val pkgVersion = readOrDefault[Int]("edata.metadata.pkgVersion", 0)
    pkgVersion.toDouble
  }

	def validEvent(): Boolean = {
		(StringUtils.equals("publish", action) && StringUtils.isNotBlank(objectId)) && (objectTypes.contains(objectType) && mimeTypes.contains(mimeType))
	}
}