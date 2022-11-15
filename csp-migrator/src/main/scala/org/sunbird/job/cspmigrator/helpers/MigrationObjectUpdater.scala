package org.sunbird.job.cspmigrator.helpers

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.domain.Event
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}

trait MigrationObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[MigrationObjectUpdater])

  def updateContentBody(identifier: String, ecmlBody: String, config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val updateQuery = QueryBuilder.update(config.contentKeyspaceName, config.contentTableName)
      .where(QueryBuilder.eq("content_id", identifier))
      .`with`(QueryBuilder.set("body", QueryBuilder.fcall("textAsBlob", ecmlBody)))
      logger.info(s"""MigrationObjectUpdater:: updateContentBody:: Updating Content Body in Cassandra For $identifier : ${updateQuery.toString}""")
      val result = cassandraUtil.upsert(updateQuery.toString)
      if (result) logger.info(s"""MigrationObjectUpdater:: updateContentBody:: Content Body Updated Successfully For $identifier""")
      else {
        logger.error(s"""MigrationObjectUpdater:: updateContentBody:: Content Body Update Failed For $identifier""")
        throw new InvalidInputException(s"""Content Body Update Failed For $identifier""")
      }
  }

  def updateAssessmentItemData(identifier: String, updatedData: Map[String, String], config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val updateQuery = QueryBuilder.update(config.contentKeyspaceName, config.assessmentTableName)
      .where(QueryBuilder.eq("question_id", identifier))
      .`with`(QueryBuilder.set("body", QueryBuilder.fcall("textAsBlob", updatedData.getOrElse("body", null))))
      .and(QueryBuilder.set("question", QueryBuilder.fcall("textAsBlob", updatedData.getOrElse("question", null))))
      .and(QueryBuilder.set("editorstate", updatedData.getOrElse("editorState", null)))
      .and(QueryBuilder.set("solutions", updatedData.getOrElse("solutions", null)))

    logger.info(s"""MigrationObjectUpdater:: updateAssessmentItemData:: Updating Assessment Body in Cassandra For $identifier : ${updateQuery.toString}""")
    val result = cassandraUtil.upsert(updateQuery.toString)
    if (result) logger.info(s"""MigrationObjectUpdater:: updateAssessmentItemData:: Assessment Body Updated Successfully For $identifier""")
    else {
      logger.error(s"""MigrationObjectUpdater:: updateAssessmentItemData:: Assessment Body Update Failed For $identifier""")
      throw new InvalidInputException(s"""Assessment Body Update Failed For $identifier""")
    }
  }

  def updateCollectionHierarchy(identifier: String, hierarchy: String, config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val updateQuery = QueryBuilder.update(config.hierarchyKeyspaceName, config.hierarchyTableName)
      .where(QueryBuilder.eq("identifier", identifier))
      .`with`(QueryBuilder.set("hierarchy", hierarchy))
    logger.info(s"""MigrationObjectUpdater:: updateCollectionHierarchy:: Updating Hierarchy in Cassandra For $identifier : ${updateQuery.toString}""")
    val result = cassandraUtil.upsert(updateQuery.toString)
    if (result) logger.info(s"""MigrationObjectUpdater:: updateCollectionHierarchy:: Hierarchy Updated Successfully For $identifier""")
    else {
      logger.error(s"""MigrationObjectUpdater:: updateCollectionHierarchy:: Hierarchy Update Failed For $identifier""")
      throw new InvalidInputException(s"""Hierarchy Update Failed For $identifier""")
    }
  }

  def updateNeo4j(updatedMetadata: Map[String, AnyRef], event: Event)(neo4JUtil: Neo4JUtil): Unit = {
    logger.info(s"""MigrationObjectUpdater:: process:: ${event.identifier} - ${event.objectType} updated fields data:: $updatedMetadata""")
    neo4JUtil.updateNode(event.identifier, updatedMetadata)
    logger.info("MigrationObjectUpdater:: process:: static fields migration completed for " + event.identifier)
  }


}
