package org.sunbird.job.questionset.publish.util

import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.util.CloudStorageUtil
import org.sunbird.job.questionset.publish.helpers.QuestionPublisher
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}

import scala.concurrent.ExecutionContext

object QuestionPublishUtil extends QuestionPublisher {

	private val pkgTypes = List("FULL", "ONLINE")

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPublishUtil])

	def publishQuestions(identifier: String, objList: List[ObjectData])(implicit ec: ExecutionContext, neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig, config: PublishConfig, httpUtil: HttpUtil): Unit = {
		objList.foreach(qData => {
			logger.info("QuestionPublishUtil :::: publishing child question  for : " + qData.identifier)
			val obj = getObject(qData.identifier, qData.pkgVersion, readerConfig)(neo4JUtil, cassandraUtil)
			val messages: List[String] = validate(obj, obj.identifier, validateQuestion)
			if (messages.isEmpty) {
				val enrichedObj = enrichObject(obj)(neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig)
				val objWithEcar = getObjectWithEcar(enrichedObj, pkgTypes)(ec, cloudStorageUtil, definitionCache, definitionConfig, httpUtil)
				logger.info("Ecar generation done for Question: " + objWithEcar.identifier)
				saveOnSuccess(objWithEcar)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)
				logger.info("Question publishing completed successfully for : " + qData.identifier)
			} else {
				saveOnFailure(obj, messages)(neo4JUtil)
				logger.info("Question publishing failed for : " + qData.identifier)
			}
		})

	}
}

class QuestionPublishUtil {}
