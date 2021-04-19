package org.sunbird.job.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any]) extends JobRequest(eventMap) {

	private val jobName = "auto-creator-v2"

	private val objectTypes = List("Question", "QuestionSet")

	def eData: Map[String, AnyRef] = readOrDefault("edata", Map()).asInstanceOf[Map[String, AnyRef]]

	def metadata: Map[String, AnyRef] = readOrDefault("edata.metadata", Map())

	def collection: List[Map[String, AnyRef]] = readOrDefault("edata.collection", List(Map())).asInstanceOf[List[Map[String, AnyRef]]]

	def action: String = readOrDefault[String]("edata.action", "")

	def mimeType: String = readOrDefault[String]("edata.metadata.mimeType", "")

	def objectId: String = readOrDefault[String]("edata.metadata.identifier", "")

	def objectType: String = readOrDefault[String]("edata.objectType", "")

	def repository: String = readOrDefault[String]("edata.repository", "")

	def pkgVersion: Double = {
		val pkgVersion = readOrDefault[Int]("edata.metadata.pkgVersion", 0)
		pkgVersion.toDouble
	}

	def isValid(): Boolean = {
		val downloadUrl = metadata.getOrElse("downloadUrl", "").asInstanceOf[String]
		(StringUtils.equals("auto-create", action) && StringUtils.isNotBlank(objectId)) && (objectTypes.contains(objectType)
		  && StringUtils.isNotBlank(repository) && metadata.nonEmpty) && (StringUtils.isNotBlank(downloadUrl) && StringUtils.endsWith(downloadUrl, ".ecar"))
	}
}