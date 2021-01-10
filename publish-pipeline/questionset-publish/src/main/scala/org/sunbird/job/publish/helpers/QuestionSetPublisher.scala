package org.sunbird.job.publish.helpers

import org.sunbird.job.util.CassandraUtil
import org.sunbird.publish.core.ObjectData
import org.sunbird.publish.helpers.{ObjectEnrichment, ObjectReader, ObjectUpdater, ObjectValidator}

import scala.collection.mutable.ListBuffer

trait QuestionSetPublisher extends ObjectReader with ObjectValidator with ObjectUpdater with ObjectEnrichment {

	override def getExtData(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

	def validateQuestionSet(obj: ObjectData, identifier: String): List[String] = {
		val messages = ListBuffer[String]()
		if(obj.hierarchy.get.isEmpty) messages += s"""There is no hierarchy available for : $identifier"""
		messages.toList
	}

	def dummyFunc = (obj: ObjectData) => {}
}
