package org.sunbird.job.contentautocreator.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

	private val jobName = "content-auto-creator"

	private val contentObjectType = "Content"

	def eData: Map[String, AnyRef] = readOrDefault("edata", Map()).asInstanceOf[Map[String, AnyRef]]

	def metadata: Map[String, AnyRef] = readOrDefault("edata.metadata", Map())

	def collection: List[Map[String, String]] = readOrDefault("edata.collection", List(Map())).asInstanceOf[List[Map[String, String]]]

	def action: String = readOrDefault[String]("edata.action", "")

	def mimeType: String = readOrDefault[String]("edata.metadata.mimeType", "")

	def objectId: String = readOrDefault[String]("edata.metadata.identifier", "")

	def objectType: String = readOrDefault[String]("edata.objectType", "")

	def repository: Option[String] = read[String]("edata.repository")

	def downloadUrl: String = readOrDefault[String]("edata.metadata.downloadUrl", "")

	def pkgVersion: Double = {
		val pkgVersion = readOrDefault[Int]("edata.metadata.pkgVersion", 0)
		pkgVersion.toDouble
	}

	def isValid: Boolean = {
		(StringUtils.equals("auto-create", action) && StringUtils.isNotBlank(objectId)) && (contentObjectType.contains(objectType)
		  && repository.nonEmpty && metadata.nonEmpty) && (StringUtils.isNotBlank(downloadUrl) && StringUtils.endsWith(downloadUrl, ".ecar"))
	}
}