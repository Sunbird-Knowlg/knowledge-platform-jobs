package org.sunbird.publish.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.util.Neo4JUtil
import org.sunbird.publish.core.ObjectData

import java.text.SimpleDateFormat
import java.util.Date

trait ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectUpdater])

  def saveOnSuccess(obj: ObjectData, extSaveFn: (ObjectData) => Unit)(implicit neo4JUtil: Neo4JUtil): Unit = {
    val publishType = obj.metadata.getOrElse("publish_type", "Public").asInstanceOf[String]
    val status = if (StringUtils.equals("Private", publishType)) "Unlisted" else "Live"
    val nodeId = obj.metadata.get("IL_UNIQUE_ID").get
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$nodeId"}) SET n.status="$status" $auditPropsUpdateQuery();"""
    logger.info("Query: " + query)
    neo4JUtil.executeQuery(query)
    extSaveFn(obj)
  }

  def saveOnFailure(obj: ObjectData, messages: List[String])(implicit neo4JUtil: Neo4JUtil): Unit = {
    val errorMessages = messages.mkString("; ")
    val nodeId = obj.metadata.get("IL_UNIQUE_ID").get
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$nodeId"}) SET n.status="Failed", n.publishError="$errorMessages" $auditPropsUpdateQuery();"""
    logger.info("Query: " + query)
    neo4JUtil.executeQuery(query)
  }

  private def auditPropsUpdateQuery(): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    val updatedOn = sdf.format(new Date())
    s""" n.lastUpdatedOn=$updatedOn """
  }

}
