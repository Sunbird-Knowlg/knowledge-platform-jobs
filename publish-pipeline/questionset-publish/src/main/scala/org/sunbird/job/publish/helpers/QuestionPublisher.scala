package org.sunbird.job.publish.helpers

import org.sunbird.job.util.CassandraUtil
import org.sunbird.publish.helpers.{ObjectEnrichment, ObjectReader, ObjectUpdater, ObjectValidator}

trait QuestionPublisher extends ObjectReader with ObjectValidator with ObjectEnrichment with ObjectUpdater {

	override def getExtData(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		Option(Map[String, AnyRef]())
	}

	def getHierarchy(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None
}
