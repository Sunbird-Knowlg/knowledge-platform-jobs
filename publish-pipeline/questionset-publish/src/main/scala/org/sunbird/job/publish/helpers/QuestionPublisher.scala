package org.sunbird.job.publish.helpers

import org.sunbird.job.util.CassandraUtil
import org.sunbird.publish.helpers.{ObjectReader, ObjectValidator}

trait QuestionPublisher extends ObjectReader with ObjectValidator {

	override def getExtData(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		//TODO: Implement this method
		None
	}

	def getHierarchy(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

	//TODO: additional method for question publish goes here
}
