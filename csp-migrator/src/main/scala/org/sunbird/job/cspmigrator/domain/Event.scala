package org.sunbird.job.cspmigrator.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.cspmigrator.models.ObjectData
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

	val jobName = "content-auto-creator"

	def eData: Map[String, AnyRef] = readOrDefault("edata", Map()).asInstanceOf[Map[String, AnyRef]]

	def context: Map[String, AnyRef] = readOrDefault("context", Map()).asInstanceOf[Map[String, AnyRef]]

	def obj: Map[String, AnyRef] = readOrDefault("object", Map()).asInstanceOf[Map[String, AnyRef]]

	def channel: String = readOrDefault[String]("context.channel", "")

	def metadata: Map[String, AnyRef] = readOrDefault("edata.metadata", Map())

	def action: String = readOrDefault[String]("edata.action", "")

	def mimeType: String = readOrDefault[String]("edata.metadata.mimeType", "")

	def objectType: String = readOrDefault[String]("edata.objectType", "")

	def identifier: String = readOrDefault[String]("object.id", "")

	def currentIteration: Int = readOrDefault[Int]("edata.iteration", 1)

	def pkgVersion: Double = {
		val pkgVersion = readOrDefault[Int]("edata.metadata.pkgVersion", 0)
		pkgVersion.toDouble
	}

	def isValid(objMetadata: Map[String, AnyRef], config: CSPMigratorConfig): Boolean = {
		(objMetadata.getOrElse("migrationVersion",0).asInstanceOf[Number].doubleValue() == 0 && StringUtils.equals("csp-migration", action) && StringUtils.isNotBlank(identifier)) && StringUtils.isNotBlank(channel)
	}

}