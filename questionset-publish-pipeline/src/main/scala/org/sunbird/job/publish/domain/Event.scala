package org.sunbird.job.publish.domain

import java.util

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest
import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

	private val jobName = "questionset-publish-pipeline"

	def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asScala.toMap

	def action: String = readOrDefault[String]("edata.action", "")

	def mimeType: String = readOrDefault[String]("edata.metadata.mimeType", "")

	def objectId: String = readOrDefault[String]("edata.metadata.identifier", "")

	def objectType: String = readOrDefault[String]("edata.metadata.objectType", "")

	def validEvent(): Boolean = {
		(StringUtils.equals("publish", action) && StringUtils.isNotBlank(objectId)) && (StringUtils.equals("QuestionSet", objectType) && StringUtils.equals("application/vnd.sunbird.questionset", mimeType))
	}

}
