package org.sunbird.job.publish.helpers

import org.sunbird.job.util.CassandraUtil
import org.sunbird.publish.helpers.{ObjectEnrichment, ObjectReader, ObjectUpdater, ObjectValidator}

trait QuestionSetPublisher extends ObjectReader with ObjectValidator with ObjectUpdater with ObjectEnrichment {

	override def getExtData(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

	def getHierarchy(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		//TODO: Implement the method
		None
	}

	//TODO: additional method for questionset publish goes here
}
