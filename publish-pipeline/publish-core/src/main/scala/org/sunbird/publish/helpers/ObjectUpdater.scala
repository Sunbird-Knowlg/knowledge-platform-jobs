package org.sunbird.publish.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.util.{CassandraUtil, JSONUtil, Neo4JUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}
import java.text.SimpleDateFormat
import java.util.Date

trait ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectUpdater])

  @throws[Exception]
  def saveOnSuccess(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): Unit = {
    val publishType = obj.metadata.getOrElse("publish_type", "Public").asInstanceOf[String]
    val status = if (StringUtils.equals("Private", publishType)) "Unlisted" else "Live"
    val editId = obj.dbId
    val identifier = obj.identifier
    //val newPkgVersion = obj.pkgVersion + 1
    val metadataUpdateQuery = metaDataQuery(obj)
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$identifier"}) SET n.status="$status", n.pkgVersion=$obj.pkgVersion, $metadataUpdateQuery, $auditPropsUpdateQuery;"""
    logger.info("Query: " + query)
    if (!StringUtils.equalsIgnoreCase(editId, identifier)) {
      val imgNodeDelQuery = s"""MATCH (n:domain{IL_UNIQUE_ID:"$editId"}) DETACH DELETE n;"""
      neo4JUtil.executeQuery(imgNodeDelQuery)
    }
    neo4JUtil.executeQuery(query)
    saveExternalData(obj, readerConfig)
  }

  def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil)

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
