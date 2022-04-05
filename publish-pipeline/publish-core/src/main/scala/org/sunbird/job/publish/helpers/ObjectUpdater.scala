package org.sunbird.job.publish.helpers

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.apache.commons.lang3.StringUtils
import org.neo4j.driver.v1.StatementResult
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.util.{CassandraUtil, JSONUtil, Neo4JUtil, ScalaJsonUtil}

import java.text.SimpleDateFormat
import java.util
import java.util.Date

trait ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectUpdater])

  @throws[Exception]
  def saveOnSuccess(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, definitionCache: DefinitionCache, config: DefinitionConfig): Unit = {
    val publishType = obj.getString("publish_type", "Public")
    val status = if (StringUtils.equalsIgnoreCase("Unlisted", publishType)) "Unlisted" else "Live"
    val editId = obj.dbId
    val identifier = obj.identifier
    val metadataUpdateQuery = metaDataQuery(obj)(definitionCache, config)
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$identifier"}) SET n.status="$status",n.pkgVersion=${obj.pkgVersion},n.prevStatus="Processing",$metadataUpdateQuery,$auditPropsUpdateQuery;"""
    logger.info("ObjectUpdater:: saveOnSuccess:: Query: " + query)

    if (obj.mimeType.equalsIgnoreCase("application/vnd.ekstep.ecml-archive")) {
      val ecmlBody = getContentBody(identifier, readerConfig)
      updateContentBody(identifier,ecmlBody,readerConfig)
    }

    if (!StringUtils.equalsIgnoreCase(editId, identifier)) {
      val imgNodeDelQuery = s"""MATCH (n:domain{IL_UNIQUE_ID:"$editId"}) DETACH DELETE n;"""
      neo4JUtil.executeQuery(imgNodeDelQuery)
      deleteExternalData(obj, readerConfig)
    }
    val result: StatementResult = neo4JUtil.executeQuery(query)
    if (null != result && result.hasNext)
      logger.info(s"ObjectUpdater:: saveOnSuccess:: statement result : ${result.next().asMap()}")
    saveExternalData(obj, readerConfig)
  }

  @throws[Exception]
  def updateProcessingNode(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, definitionCache: DefinitionCache, config: DefinitionConfig): Unit = {
    val status = "Processing"
    val prevState = obj.getString("status", "Draft")
    val identifier = obj.dbId
    val metadataUpdateQuery = metaDataQuery(obj)(definitionCache, config)
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$identifier"}) SET n.status="$status",n.prevState="$prevState",$metadataUpdateQuery,$auditPropsUpdateQuery;"""
    logger.info("ObjectUpdater:: updateProcessingNode:: Query: " + query)
    val result: StatementResult = neo4JUtil.executeQuery(query)
    if (null != result && result.hasNext)
      logger.info(s"ObjectUpdater:: updateProcessingNode:: statement result : ${result.next().asMap()}")
  }

  def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit

  def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit

  @throws[Exception]
  def saveOnFailure(obj: ObjectData, messages: List[String], pkgVersion: Double)(implicit neo4JUtil: Neo4JUtil): Unit = {
    val upPkgVersion = pkgVersion + 1
    val errorMessages = messages.mkString("; ")
    val nodeId = obj.dbId
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$nodeId"}) SET n.status="Failed", n.pkgVersion=$upPkgVersion, n.publishError="$errorMessages", $auditPropsUpdateQuery;"""
    logger.info("ObjectUpdater:: saveOnFailure:: Query: " + query)
    neo4JUtil.executeQuery(query)
  }

  def metaDataQuery(obj: ObjectData)(definitionCache: DefinitionCache, config: DefinitionConfig): String = {
    val version = config.supportedVersion.getOrElse(obj.dbObjType.toLowerCase(), "1.0").asInstanceOf[String]
    val definition = definitionCache.getDefinition(obj.dbObjType, version, config.basePath)
    val metadata = obj.metadata - ("IL_UNIQUE_ID", "identifier", "IL_FUNC_OBJECT_TYPE", "IL_SYS_NODE_TYPE", "pkgVersion", "lastStatusChangedOn", "lastUpdatedOn", "status", "objectType", "publish_type")
    metadata.map(prop => {
      if (null == prop._2) s"n.${prop._1}=${prop._2}"
      else if (definition.objectTypeProperties.contains(prop._1)) {
        prop._2 match {
          case _: Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(ScalaJsonUtil.serialize(prop._2))
            s"""n.${prop._1}=$strValue"""
          case _: util.Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(JSONUtil.serialize(prop._2))
            s"""n.${prop._1}=$strValue"""
          case _ =>
            val strValue = JSONUtil.serialize(prop._2)
            s"""n.${prop._1}=$strValue"""
        }
      } else {
        prop._2 match {
          case _: Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(ScalaJsonUtil.serialize(prop._2))
            s"""n.${prop._1}=$strValue"""
          case _: util.Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(JSONUtil.serialize(prop._2))
            s"""n.${prop._1}=$strValue"""
          case _: List[String] =>
            val strValue = ScalaJsonUtil.serialize(prop._2)
            s"""n.${prop._1}=$strValue"""
          case _: util.List[String] =>
            val strValue = JSONUtil.serialize(prop._2)
            s"""n.${prop._1}=$strValue"""
          case _ =>
            val strValue = JSONUtil.serialize(prop._2)
            s"""n.${prop._1}=$strValue"""
        }
      }
    }).mkString(",")
  }

  private def auditPropsUpdateQuery(): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val updatedOn = sdf.format(new Date())
    s"""n.lastUpdatedOn="$updatedOn",n.lastStatusChangedOn="$updatedOn""""
  }

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

}
