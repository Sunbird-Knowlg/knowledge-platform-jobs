package org.sunbird.job.cspmigrator.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.apache.commons.lang3
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.models.{ExtDataConfig, ObjectData}
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.util.CassandraUtil

trait ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectUpdater])
  val extProps = List("body", "editorState", "answer", "solutions", "instructions", "hints", "media", "responseDeclaration", "interactions", "identifier")

  def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit

  def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit

  def getContentBody(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): String = {
    // fetch content body from cassandra
    val select = QueryBuilder.select()
    select.fcall("blobAsText", QueryBuilder.column("body")).as("body")
    val selectWhere: Select.Where = select.from(readerConfig.keyspace, readerConfig.table).where().and(QueryBuilder.eq("content_id", identifier + ".img"))
    logger.info("ObjectUpdater:: getContentBody:: Cassandra Fetch Query for image:: " + selectWhere.toString)
    val row = cassandraUtil.findOne(selectWhere.toString)
    if (null != row) row.getString("body") else {
      val selectId = QueryBuilder.select()
      selectId.fcall("blobAsText", QueryBuilder.column("body")).as("body")
      val selectWhereId: Select.Where = selectId.from(readerConfig.keyspace, readerConfig.table).where().and(QueryBuilder.eq("content_id", identifier))
      logger.info("ObjectUpdater:: getContentBody:: Cassandra Fetch Query :: " + selectWhereId.toString)
      val rowId = cassandraUtil.findOne(selectWhereId.toString)
      if (null != rowId) rowId.getString("body") else ""
    }
  }

  def updateContentBody(identifier: String, ecmlBody: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val updateQuery = QueryBuilder.update(readerConfig.keyspace, readerConfig.table)
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


  def getQuestionData(identifier: String, config: CSPMigratorConfig)(implicit cassandraUtil: CassandraUtil): Row = {
    logger.info("QuestionPublisher ::: getQuestionData ::: Reading Question External Data For : " + identifier)
    val select = QueryBuilder.select()
    extProps.foreach(prop => if (lang3.StringUtils.equals("body", prop) | lang3.StringUtils.equals("answer", prop)) select.fcall("blobAsText", QueryBuilder.column(prop.toLowerCase())).as(prop.toLowerCase()) else select.column(prop.toLowerCase()).as(prop.toLowerCase()))
    val selectWhere: Select.Where = select.from(config.contentKeyspaceName, config.assessmentTableName).where().and(QueryBuilder.eq("identifier", identifier))
    logger.info("Cassandra Fetch Query :: " + selectWhere.toString)
    cassandraUtil.findOne(selectWhere.toString)
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
