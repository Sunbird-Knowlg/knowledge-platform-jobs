package org.sunbird.publish.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.util.{CassandraUtil, JSONUtil, Neo4JUtil, ScalaJsonUtil}
import org.sunbird.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.neo4j.driver.v1.StatementResult
import org.sunbird.job.domain.`object`.DefinitionCache

trait ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectUpdater])

  @throws[Exception]
  def saveOnSuccess(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, definitionCache: DefinitionCache, config: DefinitionConfig): Unit = {
    val publishType = obj.metadata.getOrElse("publish_type", "Public").asInstanceOf[String]
    val status = if (StringUtils.equals("Private", publishType)) "Unlisted" else "Live"
    val editId = obj.dbId
    val identifier = obj.identifier
    val metadataUpdateQuery = metaDataQuery(obj)(definitionCache, config)
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$identifier"}) SET n.status="$status",n.pkgVersion=${obj.pkgVersion},$metadataUpdateQuery,$auditPropsUpdateQuery;"""
    logger.info("Query: " + query)
    if (!StringUtils.equalsIgnoreCase(editId, identifier)) {
      val imgNodeDelQuery = s"""MATCH (n:domain{IL_UNIQUE_ID:"$editId"}) DETACH DELETE n;"""
      neo4JUtil.executeQuery(imgNodeDelQuery)
      deleteExternalData(obj, readerConfig);
    }
    val result: StatementResult = neo4JUtil.executeQuery(query)
    if (null != result && result.hasNext)
      logger.info(s"statement result : ${result.next().asMap()}")
    saveExternalData(obj, readerConfig)
  }

  def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil)

  def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil)

  @throws[Exception]
  def saveOnFailure(obj: ObjectData, messages: List[String])(implicit neo4JUtil: Neo4JUtil): Unit = {
    val errorMessages = messages.mkString("; ")
    val nodeId = obj.metadata.get("IL_UNIQUE_ID").get
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$nodeId"}) SET n.status="Failed", n.publishError="$errorMessages", $auditPropsUpdateQuery;"""
    logger.info("Query: " + query)
    neo4JUtil.executeQuery(query)
  }

  def metaDataQuery(obj: ObjectData)(definitionCache: DefinitionCache, config: DefinitionConfig): String = {
    val version = config.supportedVersion.getOrElse(obj.dbObjType.toLowerCase(), "1.0").asInstanceOf[String]
    val definition = definitionCache.getDefinition(obj.dbObjType, version, config.basePath)
    val metadata = obj.metadata - ("IL_UNIQUE_ID", "identifier", "IL_FUNC_OBJECT_TYPE", "IL_SYS_NODE_TYPE", "pkgVersion", "lastStatusChangedOn", "lastUpdatedOn", "status", "objectType")
    metadata.map(prop => {
      if (null == prop._2) s"n.${prop._1}=${prop._2}"
      else if (definition.objectTypeProperties.contains(prop._1)) {
        prop._2 match {
          case _: Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(ScalaJsonUtil.serialize(prop._2))
            s"""n.${prop._1}=${strValue}"""
          case _: util.Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(JSONUtil.serialize(prop._2))
            s"""n.${prop._1}=${strValue}"""
          case _: String =>
            val strValue = JSONUtil.serialize(prop._2)
            s"""n.${prop._1}=${strValue}"""
        }
      } else {
        prop._2 match {
          case _: Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(ScalaJsonUtil.serialize(prop._2))
            s"""n.${prop._1}=${strValue}"""
          case _: util.Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(JSONUtil.serialize(prop._2))
            s"""n.${prop._1}=${strValue}"""
          case _: List[String] =>
            val strValue = ScalaJsonUtil.serialize(prop._2)
            s"""n.${prop._1}=${strValue}"""
          case _: String =>
            s"""n.${prop._1}="${prop._2}""""
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
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    val updatedOn = sdf.format(new Date())
    s"""n.lastUpdatedOn="$updatedOn",n.lastStatusChangedOn="$updatedOn""""
  }

}
