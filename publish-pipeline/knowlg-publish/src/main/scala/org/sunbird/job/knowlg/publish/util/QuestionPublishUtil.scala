package org.sunbird.job.knowlg.publish.util

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.knowlg.publish.helpers.QuestionPublisher
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, JanusGraphUtil}

import scala.concurrent.ExecutionContext

object QuestionPublishUtil extends QuestionPublisher {

  private val pkgTypes = List(EcarPackageType.FULL.toString, EcarPackageType.ONLINE.toString)

  private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPublishUtil])

  def publishQuestions(identifier: String, objList: List[ObjectData], pkgVersion: Double, lastPublishedBy: String)(implicit ec: ExecutionContext, janusGraphUtil: JanusGraphUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig, config: KnowlgPublishConfig, httpUtil: HttpUtil, eventContext: Map[String, AnyRef]): List[ObjectData] = {
    val featureName = eventContext.getOrElse("featureName", "").asInstanceOf[String]
    logger.info(s" Feature: ${featureName} | QuestionPublishUtil :::: publishing child question for questionset : " + identifier)
    objList.map(qData => {
      logger.info(s"Feature: ${featureName} | QuestionPublishUtil :::: publishing child question : " + qData.identifier)
      val objData = getObject(qData.identifier, qData.pkgVersion, qData.mimeType, qData.metadata.getOrElse("publish_type", "Public").toString, readerConfig)(janusGraphUtil, cassandraUtil, config)
      val obj = if (StringUtils.isNotBlank(lastPublishedBy)) {
        val newMeta = objData.metadata ++ Map("lastPublishedBy" -> lastPublishedBy)
        new ObjectData(objData.identifier, newMeta, objData.extData, objData.hierarchy)
      } else objData
      val messages: List[String] = validate(obj, obj.identifier, validateQuestion)
      if (messages.isEmpty) {
        val enrichedObj = enrichObject(obj)(janusGraphUtil, cassandraUtil, readerConfig, cloudStorageUtil, config, definitionCache, definitionConfig)
        val objWithArtifactUrl = if (enrichedObj.getString("artifactUrl", "").isEmpty) {
          //create artifact zip locally, upload to cloud and update the artifact URL
          updateArtifactUrl(enrichedObj, EcarPackageType.FULL.toString)(ec, janusGraphUtil, cloudStorageUtil, definitionCache, definitionConfig, config, httpUtil)
        } else enrichedObj
        val objWithEcar = getObjectWithEcar(objWithArtifactUrl, pkgTypes)(ec, janusGraphUtil, cloudStorageUtil, config, definitionCache, definitionConfig, httpUtil)
        logger.info(s"Feature: ${featureName} | Ecar generation done for Question: " + objWithEcar.identifier)
        saveOnSuccess(objWithEcar)(janusGraphUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig, config)
        logger.info(s"Feature: ${featureName} | Question publishing completed successfully for : " + qData.identifier)
        objWithEcar
      } else {
        saveOnFailure(obj, messages, pkgVersion)(janusGraphUtil)
        logger.info(s"Feature: ${featureName} | Question publishing failed for : " + qData.identifier)
        obj
      }
    })
  }
}

class QuestionPublishUtil {}