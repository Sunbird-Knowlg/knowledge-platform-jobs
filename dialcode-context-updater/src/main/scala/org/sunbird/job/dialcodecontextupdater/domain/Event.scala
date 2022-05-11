package org.sunbird.job.dialcodecontextupdater.domain

import org.sunbird.job.dialcodecontextupdater.task.DialcodeContextUpdaterConfig
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

	val jobName = "content-auto-creator"

	def eData: Map[String, AnyRef] = readOrDefault("edata", Map()).asInstanceOf[Map[String, AnyRef]]

	def context: Map[String, AnyRef] = readOrDefault("context", Map()).asInstanceOf[Map[String, AnyRef]]

	def obj: Map[String, AnyRef] = readOrDefault("object", Map()).asInstanceOf[Map[String, AnyRef]]

	def channel: String = readOrDefault[String]("context.channel", "")

	def metadata: Map[String, AnyRef] = readOrDefault("edata.metadata", Map())

	def collection: List[Map[String, String]] = readOrDefault("edata.collection", List(Map())).asInstanceOf[List[Map[String, String]]]

	def action: String = readOrDefault[String]("edata.action", "")

	def mimeType: String = readOrDefault[String]("edata.metadata.mimeType", "")

	def objectId: String = readOrDefault[String]("object.id", "")

	def objectType: String = readOrDefault[String]("edata.objectType", "")

	def repository: Option[String] = read[String]("edata.repository")

	def artifactUrl: String = readOrDefault[String]("edata.metadata.artifactUrl", "")

	def stage: String = readOrDefault[String]("edata.stage", "")

	def identifier: String = readOrDefault[String]("object.id", "")

	def reqOriginData: Map[String, String] = readOrDefault("edata.originData", Map()).asInstanceOf[Map[String, String]]

	def currentIteration: Int = readOrDefault[Int]("edata.iteration", 1)

	def pkgVersion: Double = {
		val pkgVersion = readOrDefault[Int]("edata.metadata.pkgVersion", 0)
		pkgVersion.toDouble
	}

	def isValid(config: DialcodeContextUpdaterConfig): Boolean = {
		true
	}
}