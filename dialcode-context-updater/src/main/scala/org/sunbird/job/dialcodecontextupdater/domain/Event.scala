package org.sunbird.job.dialcodecontextupdater.domain

import org.sunbird.job.dialcodecontextupdater.task.DialcodeContextUpdaterConfig
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

	val jobName = "dialcode-context-updater"

	def eData: Map[String, AnyRef] = readOrDefault("edata", Map()).asInstanceOf[Map[String, AnyRef]]

	def obj: Map[String, AnyRef] = readOrDefault("object", Map()).asInstanceOf[Map[String, AnyRef]]

	def channel: String = readOrDefault[String]("context.channel", "")

	def action: String = readOrDefault[String]("edata.action", "")

	def dialcode: String = readOrDefault[String]("edata.dialcode", "")

	def identifier: String = readOrDefault[String]("edata.identifier", "")


	def isValid(): Boolean = {
				dialcode.nonEmpty
	}
}