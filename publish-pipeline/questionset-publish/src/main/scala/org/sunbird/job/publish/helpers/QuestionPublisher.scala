package org.sunbird.job.publish.helpers

import java.util

import org.slf4j.LoggerFactory
import org.sunbird.job.util.CassandraUtil
import org.sunbird.publish.core.ObjectData
import org.sunbird.publish.helpers.{ObjectEnrichment, ObjectReader, ObjectUpdater, ObjectValidator}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

trait QuestionPublisher extends ObjectReader with ObjectValidator with ObjectEnrichment with ObjectUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPublisher])

	def getHierarchy(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

	def validateQuestion(obj: ObjectData, identifier: String): List[String] = {
		logger.info("Validating Question External Data For : "+obj.identifier)
		//TODO: Remove Below logger statement
		logger.info("Question External Data For : "+obj.extData.get)
		val messages = ListBuffer[String]()
		if (obj.extData.get.getOrElse("body", "").asInstanceOf[String].isEmpty) messages += s"""There is no body available for : $identifier"""
		if (obj.extData.get.getOrElse("editorState", "").asInstanceOf[String].isEmpty) messages += s"""There is no editorState available for : $identifier"""
		val interactionTypes = obj.metadata.getOrElse("interactionTypes", new util.ArrayList[String]()).asInstanceOf[util.List[String]].asScala.toList
		interactionTypes.nonEmpty match {
			case true => {
				if (obj.extData.get.getOrElse("responseDeclaration", "").asInstanceOf[String].isEmpty) messages += s"""There is no responseDeclaration available for : $identifier"""
				if (obj.extData.get.getOrElse("interactions", "").asInstanceOf[String].isEmpty) messages += s"""There is no interactions available for : $identifier"""
			}
			case false => if (obj.extData.getOrElse("answer", "").toString.isEmpty) messages += s"""There is no answer available for : $identifier"""
		}
		messages.toList
	}

	def dummyFunc = (obj: ObjectData) => {}
}
