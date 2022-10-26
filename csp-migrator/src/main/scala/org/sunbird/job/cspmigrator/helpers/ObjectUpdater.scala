package org.sunbird.job.cspmigrator.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.apache.commons.lang3
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.util.CassandraUtil

trait ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectUpdater])

  def updateContentBody(identifier: String, ecmlBody: String, config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val updateQuery = QueryBuilder.update(config.contentKeyspaceName, config.contentTableName)
      .where(QueryBuilder.eq("content_id", identifier))
      .`with`(QueryBuilder.set("body", QueryBuilder.fcall("textAsBlob", ecmlBody)))
      logger.info(s"ObjectUpdater:: updateContentBody:: Updating Content Body in Cassandra For $identifier : ${updateQuery.toString}")
      val result = cassandraUtil.upsert(updateQuery.toString)
      if (result) logger.info(s"ObjectUpdater:: updateContentBody:: Content Body Updated Successfully For $identifier")
      else {
        logger.error(s"ObjectUpdater:: updateContentBody:: Content Body Update Failed For $identifier")
        throw new InvalidInputException(s"Content Body Update Failed For $identifier")
      }
  }

  def updateQuestionData(identifier: String, updatedData: Map[String, String], config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val updateQuery = QueryBuilder.update(config.contentKeyspaceName, config.assessmentTableName)
      .where(QueryBuilder.eq("identifier", identifier))
      .`with`(QueryBuilder.set("body", QueryBuilder.fcall("textAsBlob", updatedData.getOrElse("body", null))))
      .and(QueryBuilder.set("answer", QueryBuilder.fcall("textAsBlob", updatedData.getOrElse("answer", null))))
      .and(QueryBuilder.set("editorstate", updatedData.getOrElse("editorState", null)))
      .and(QueryBuilder.set("solutions", updatedData.getOrElse("solutions", null)))
      .and(QueryBuilder.set("instructions", updatedData.getOrElse("instructions", null)))
      .and(QueryBuilder.set("media", updatedData.getOrElse("media", null)))
      .and(QueryBuilder.set("hints", updatedData.getOrElse("hints", null)))
      .and(QueryBuilder.set("responsedeclaration", updatedData.getOrElse("responseDeclaration", null)))
      .and(QueryBuilder.set("interactions", updatedData.getOrElse("interactions", null)))

    logger.info(s"ObjectUpdater:: updateAssessmentBody:: Updating Assessment Body in Cassandra For $identifier : ${updateQuery.toString}")
    val result = cassandraUtil.upsert(updateQuery.toString)
    if (result) logger.info(s"ObjectUpdater:: updateAssessmentBody:: Assessment Body Updated Successfully For $identifier")
    else {
      logger.error(s"ObjectUpdater:: updateAssessmentBody:: Assessment Body Update Failed For $identifier")
      throw new InvalidInputException(s"Assessment Body Update Failed For $identifier")
    }
  }

}
