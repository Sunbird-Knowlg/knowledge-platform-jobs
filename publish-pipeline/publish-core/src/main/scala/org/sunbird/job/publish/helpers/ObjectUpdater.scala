package org.sunbird.job.publish.helpers

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.util.{CSPMetaUtil, CassandraUtil, JanusGraphUtil, ScalaJsonUtil}

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.collection.JavaConverters._

trait ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectUpdater])

  @throws[Exception]
  def saveOnSuccess(obj: ObjectData)(implicit janusGraphUtil: JanusGraphUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig, config: PublishConfig): Unit = {
    val publishType = obj.getString("publish_type", "Public")
    val status = if (StringUtils.equalsIgnoreCase("Unlisted", publishType)) "Unlisted" else "Live"
    val editId = obj.dbId
    val identifier = obj.identifier
    val metadataUpdate = getMetadata(obj)(definitionCache, definitionConfig)
    val finalMetadata = metadataUpdate ++ Map("status" -> status, "pkgVersion" -> obj.pkgVersion, "prevStatus" -> "Processing") ++ getAuditProps()
    
    logger.info(s"ObjectUpdater:: saveOnSuccess:: Updating node ${obj.identifier} with DB ID ${obj.dbId} | pkgVersion : ${obj.pkgVersion}" )

    if (obj.mimeType.equalsIgnoreCase("application/vnd.ekstep.ecml-archive")) {
      val ecmlBody = getContentBody(identifier, readerConfig)
      updateContentBody(identifier,ecmlBody,readerConfig)
    }

    if (!StringUtils.equalsIgnoreCase(editId, identifier)) {
      janusGraphUtil.deleteNode(editId)
      deleteExternalData(obj, readerConfig)
      logger.info(s"Image Node Data Is Deleted Successfully For ${editId}")
    }
    
    janusGraphUtil.updateNode(identifier, finalMetadata.asInstanceOf[Map[String, AnyRef]])
    saveExternalData(obj, readerConfig)
  }

  @throws[Exception]
  def updateProcessingNode(obj: ObjectData)(implicit janusGraphUtil: JanusGraphUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, definitionCache: DefinitionCache, config: DefinitionConfig): Unit = {
    val status = "Processing"
    val prevState = obj.getString("status", "Draft")
    val identifier = obj.dbId
    val metadataUpdate = getMetadata(obj)(definitionCache, config)
    val finalMetadata = metadataUpdate ++ Map("status" -> status, "prevState" -> prevState) ++ getAuditProps()
    
    logger.info(s"ObjectUpdater:: updateProcessingNode:: Updating node $identifier to Processing status")
    janusGraphUtil.updateNode(identifier, finalMetadata.asInstanceOf[Map[String, AnyRef]])
  }

  def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit

  def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit

  @throws[Exception]
  def saveOnFailure(obj: ObjectData, messages: List[String], pkgVersion: Double)(implicit janusGraphUtil: JanusGraphUtil): Unit = {
    val upPkgVersion = pkgVersion + 1
    val errorMessages = messages.mkString("; ")
    val nodeId = obj.dbId
    val finalMetadata = Map("status" -> "Failed", "pkgVersion" -> upPkgVersion, "publishError" -> errorMessages) ++ getAuditProps()
    
    logger.info(s"ObjectUpdater:: saveOnFailure:: Updating node $nodeId to Failed status")
    janusGraphUtil.updateNode(nodeId, finalMetadata.asInstanceOf[Map[String, AnyRef]])
  }

  def getMetadata(obj: ObjectData)(definitionCache: DefinitionCache, config: DefinitionConfig): Map[String, AnyRef] = {
    val version = config.supportedVersion.getOrElse(obj.dbObjType.toLowerCase(), "1.0").asInstanceOf[String]
    val definition = definitionCache.getDefinition(obj.dbObjType, version, config.basePath)
    val metadata = obj.metadata - ("IL_UNIQUE_ID", "identifier", "IL_FUNC_OBJECT_TYPE", "IL_SYS_NODE_TYPE", "pkgVersion", "lastStatusChangedOn", "lastUpdatedOn", "status", "objectType", "publish_type")
    
    metadata.map(prop => {
      if (null == prop._2) (prop._1 -> prop._2)
      else if (definition.objectTypeProperties.contains(prop._1)) {
        prop._2 match {
          case _: Map[String, AnyRef] =>
             (prop._1 -> ScalaJsonUtil.serialize(prop._2))
          case _: util.Map[String, AnyRef] =>
             (prop._1 -> ScalaJsonUtil.serialize(prop._2))
          case _: List[String] | _: util.List[String] =>
             (prop._1 -> ScalaJsonUtil.serialize(prop._2))
          case _ =>
             (prop._1 -> prop._2)
        }
      } else {
        prop._2 match {
          case _: Map[String, AnyRef] | _: util.Map[String, AnyRef] =>
            (prop._1 -> ScalaJsonUtil.serialize(prop._2))
          case _: List[String] | _: util.List[String] =>
            (prop._1 -> ScalaJsonUtil.serialize(prop._2))
          case _ =>
            (prop._1 -> prop._2)
        }
      }
    })
  }

  private def getAuditProps(): Map[String, String] = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val updatedOn = sdf.format(new Date())
    Map("lastUpdatedOn" -> updatedOn, "lastStatusChangedOn" -> updatedOn)
  }

  def getContentBody(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, config: PublishConfig): String = {
    // fetch content body from cassandra
    val select = QueryBuilder.select()
    select.fcall("blobAsText", QueryBuilder.column("body")).as("body")
    val selectWhere: Select.Where = select.from(readerConfig.keyspace, readerConfig.table).where().and(QueryBuilder.eq("content_id", identifier + ".img"))
    logger.info("ObjectUpdater:: getContentBody:: Cassandra Fetch Query for image:: " + selectWhere.toString)
    val row = cassandraUtil.findOne(selectWhere.toString)
    if (null != row) {
      val body = row.getString("body")
      val updatedBody = if (isrRelativePathEnabled(config)) CSPMetaUtil.updateAbsolutePath(body) else body
      updatedBody
    } else {
      val selectId = QueryBuilder.select()
      selectId.fcall("blobAsText", QueryBuilder.column("body")).as("body")
      val selectWhereId: Select.Where = selectId.from(readerConfig.keyspace, readerConfig.table).where().and(QueryBuilder.eq("content_id", identifier))
      logger.info("ObjectUpdater:: getContentBody:: Cassandra Fetch Query :: " + selectWhereId.toString)
      val rowId = cassandraUtil.findOne(selectWhereId.toString)
      if (null != rowId) {
        val body = rowId.getString("body")
        val updatedBody = if (isrRelativePathEnabled(config)) CSPMetaUtil.updateAbsolutePath(body) else body
        updatedBody
      } else ""
    }
  }

  private def isrRelativePathEnabled(config: PublishConfig): Boolean = {
    config.getBoolean("cloudstorage.metadata.replace_absolute_path", false)
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
