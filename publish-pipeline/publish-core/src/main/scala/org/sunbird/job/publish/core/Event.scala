package org.sunbird.job.publish.core

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

	private val jobName = "chain-publish"

	private val objectTypes = List("Question", "QuestionImage", "QuestionSet", "QuestionSetImage")
	private val mimeTypes = List("application/vnd.sunbird.question", "application/vnd.sunbird.questionset")

	def eData: Map[String, AnyRef] = readOrDefault("edata", Map[String, AnyRef]())

	def context: Map[String, AnyRef] = readOrDefault("context", Map[String, AnyRef]())

	def obj: Map[String, AnyRef] = readOrDefault("object",Map[String, AnyRef]())

	def action: String = readOrDefault[String]("edata.action", "")

	def publishType: String = readOrDefault[String]("edata.publish_type", "")

	def channel : String = readOrDefault[String]("context.channel", "");

	def versionkey : String = readOrDefault[String]("object.ver", "");

	def objectIdentifier : String = readOrDefault[String]("object.id", "");

	def publishChain : List[Map[String, AnyRef]]  = readOrDefault("edata.publishchain",List[Map[String, AnyRef]]()).map(m => m.toMap).toList

	def publishChainString : String = readOrDefault[String]("edata.metadata.publishChainString", "");

	def pkgVersion: Double = {
    val pkgVersion = readOrDefault[Int]("edata.metadata.pkgVersion", 0)
    pkgVersion.toDouble
  }


	def validPublishChainEvent(): Boolean = {
		(StringUtils.equals("publishchain", action) && !publishChain.isEmpty)
	}
}
