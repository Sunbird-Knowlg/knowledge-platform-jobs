package org.sunbird.job.publish.util

import org.slf4j.LoggerFactory
import org.sunbird.job.function.QuestionPublishFunction
import org.sunbird.job.publish.helpers.QuestionPublisher
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}

object QuestionPublishUtil extends QuestionPublisher {

	//private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPublishUtil.type])

	def publishQuestions(identifier: String, objList: List[ObjectData])(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): Unit = {
		//logger.info("Question publishing started for : " + data.identifier)
		objList.foreach(qData => {
			println("QuestionPublishUtil :::: publishing child question  for : "+qData.identifier)
			val obj = getObject(qData.identifier, qData.pkgVersion, readerConfig)(neo4JUtil, cassandraUtil)
			val messages:List[String] = validate(obj, obj.identifier, validateQuestion)
			if (messages.isEmpty) {
				val enrichedObj = enrichObject(obj)(neo4JUtil, cassandraUtil, readerConfig)
				saveOnSuccess(enrichedObj, dummyFunc)(neo4JUtil)
				println("Question publishing completed successfully for : " + qData.identifier)
			} else {
				saveOnFailure(obj, messages)(neo4JUtil)
				println("Question publishing failed for : " + qData.identifier)
			}
		})

	}
}
