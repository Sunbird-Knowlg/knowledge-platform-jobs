package org.sunbird.job.publish.helpers

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.sunbird.job.util.{CassandraUtil, JSONUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}
import org.sunbird.publish.helpers.{ObjectEnrichment, ObjectReader, ObjectUpdater, ObjectValidator}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

trait QuestionSetPublisher extends ObjectReader with ObjectValidator with ObjectUpdater with ObjectEnrichment {

	override def getExtData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

	def validateQuestionSet(obj: ObjectData, identifier: String): List[String] = {
		val messages = ListBuffer[String]()
		if(obj.hierarchy.get.isEmpty) messages += s"""There is no hierarchy available for : $identifier"""
		messages.toList
	}

	override def getHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		val row = getQuestionSetHierarchy(identifier, readerConfig)
		if (null != row) {
			val data = JSONUtil.deserialize[java.util.Map[String, AnyRef]](row.getString("hierarchy"))
			Option(data.asScala.toMap)
		} else Option(Map())
	}

	def getQuestionSetHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
		val selectWhere: Select.Where = QueryBuilder.select().all()
		  .from(readerConfig.keyspace, readerConfig.table).
		  where()
		selectWhere.and(QueryBuilder.eq("identifier", identifier))
		cassandraUtil.findOne(selectWhere.toString)
	}

	def dummyFunc = (obj: ObjectData) => {}
}
