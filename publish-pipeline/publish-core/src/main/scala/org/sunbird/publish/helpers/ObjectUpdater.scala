package org.sunbird.publish.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.util.{JSONUtil, Neo4JUtil}
import org.sunbird.publish.core.ObjectData

import java.text.SimpleDateFormat
import java.util.Date

trait ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectUpdater])

  @throws[Exception]
  def saveOnSuccess(obj: ObjectData, extSaveFn: (ObjectData) => Unit)(implicit neo4JUtil: Neo4JUtil): Unit = {
    val publishType = obj.metadata.getOrElse("publish_type", "Public").asInstanceOf[String]
    val status = if (StringUtils.equals("Private", publishType)) "Unlisted" else "Live"
    val editId = obj.dbId
    val identifier = obj.identifier
    val newPkgVersion = obj.pkgVersion + 1
    val metadataUpdateQuery = metaDataQuery(obj)
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$identifier"}) SET n.status="$status", n.pkgVersion=$newPkgVersion, $metadataUpdateQuery, $auditPropsUpdateQuery;"""
    logger.info("Query: " + query)
    if (!StringUtils.equalsIgnoreCase(editId, identifier)) {
      val imgNodeDelQuery = s"""MATCH (n:domain{IL_UNIQUE_ID:"$editId"}) DETACH DELETE n;"""
      neo4JUtil.executeQuery(imgNodeDelQuery)
    }
    neo4JUtil.executeQuery(query)
    extSaveFn(obj)
  }

  @throws[Exception]
  def saveOnFailure(obj: ObjectData, messages: List[String])(implicit neo4JUtil: Neo4JUtil): Unit = {
    val errorMessages = messages.mkString("; ")
    val nodeId = obj.metadata.get("IL_UNIQUE_ID").get
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$nodeId"}) SET n.status="Failed", n.publishError="$errorMessages", $auditPropsUpdateQuery;"""
    logger.info("Query: " + query)
    neo4JUtil.executeQuery(query)
  }

  def metaDataQuery(obj: ObjectData): String = {
    val metadata = obj.metadata - ("IL_UNIQUE_ID", "identifier", "IL_FUNC_OBJECT_TYPE", "IL_SYS_NODE_TYPE", "pkgVersion", "lastStatusChangedOn", "lastUpdatedOn", "status", "objectType")
    metadata.map(prop => {
      val key = prop._1
      val value = prop._2
      value match {
        case _: String =>
          s""" n.$key="$value""""
        case _ =>
          val strValue = JSONUtil.serialize(value)
          s""" n.$key=$strValue"""
      }
    }).mkString(",")
  }


  private def auditPropsUpdateQuery(): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    val updatedOn = sdf.format(new Date())
    s""" n.lastUpdatedOn="$updatedOn", n.lastStatusChangedOn="$updatedOn""""
  }

}
