package org.sunbird.job.questionset.publish.util

import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.questionset.publish.helpers.QuestionPublisher
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, Neo4JUtil}

import scala.concurrent.ExecutionContext

object QuestionPublishUtil extends QuestionPublisher {

  private val pkgTypes = List(EcarPackageType.FULL.toString, EcarPackageType.ONLINE.toString)

  private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPublishUtil])

  def publishQuestions(identifier: String, objList: List[ObjectData], pkgVersion: Double)(implicit ec: ExecutionContext, neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig, config: PublishConfig, httpUtil: HttpUtil): List[ObjectData] = {
    logger.info("QuestionPublishUtil :::: publishing child question for questionset : " + identifier)
    objList.map(qData => {
      logger.info("QuestionPublishUtil :::: publishing child question : " + qData.identifier)
      val obj = getObject(qData.identifier, qData.pkgVersion, qData.mimeType, qData.metadata.getOrElse("publish_type", "Public").toString, readerConfig)(neo4JUtil, cassandraUtil)
      val messages: List[String] = validate(obj, obj.identifier, validateQuestion)
      if (messages.isEmpty) {
        val enrichedObj = enrichObject(obj)(neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig)
        val objWithArtifactUrl = if (enrichedObj.getString("artifactUrl", "").isEmpty) {
          //create artifact zip locally, upload to cloud and update the artifact URL
          updateArtifactUrl(enrichedObj, EcarPackageType.FULL.toString)(ec, neo4JUtil, cloudStorageUtil, definitionCache, definitionConfig, config, httpUtil)
        } else enrichedObj
        val objWithEcar = getObjectWithEcar(objWithArtifactUrl, pkgTypes)(ec, neo4JUtil, cloudStorageUtil, config, definitionCache, definitionConfig, httpUtil)
        logger.info("Ecar generation done for Question: " + objWithEcar.identifier)
        saveOnSuccess(objWithEcar)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)
        logger.info("Question publishing completed successfully for : " + qData.identifier)
        objWithEcar
      } else {
        saveOnFailure(obj, messages, pkgVersion)(neo4JUtil)
        logger.info("Question publishing failed for : " + qData.identifier)
        obj
      }
    })
  }
}

class QuestionPublishUtil {}
