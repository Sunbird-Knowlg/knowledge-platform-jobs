package org.sunbird.job.livenodepublisher.publish.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}

import scala.collection.JavaConverters._

trait LiveObjectReader {

  private[this] val logger = LoggerFactory.getLogger(classOf[LiveObjectReader])

  def getObject(identifier: String, pkgVersion: Double, mimeType: String, publishType: String, readerConfig: ExtDataConfig)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, config: PublishConfig): ObjectData = {
    logger.info("Reading editable object data for: " + identifier + " with pkgVersion: " + pkgVersion)
    val metadata = getMetadata(identifier, mimeType, publishType, pkgVersion)
    logger.info("Reading metadata for: " + identifier + " with metadata: " + metadata)
    val extData = getExtData(identifier, mimeType, readerConfig)
    logger.info("Reading extData for: " + identifier + " with extData: " + extData)
    new ObjectData(identifier, metadata, extData.getOrElse(ObjectExtData()).data, extData.getOrElse(ObjectExtData()).hierarchy)
  }

  private def getMetadata(identifier: String, mimeType: String, publishType: String, pkgVersion: Double)(implicit neo4JUtil: Neo4JUtil): Map[String, AnyRef] = {
    val metaData = neo4JUtil.getNodeProperties(identifier).asScala.toMap
    val id = metaData.getOrElse("IL_UNIQUE_ID", identifier).asInstanceOf[String]
    val objType = metaData.getOrElse("IL_FUNC_OBJECT_TYPE", "").asInstanceOf[String]
    logger.info("ObjectReader:: getMetadata:: identifier: " + identifier + " with objType: " + objType)
    metaData ++ Map[String, AnyRef]("identifier" -> id, "objectType" -> objType, "publish_type" -> publishType) - ("IL_UNIQUE_ID", "IL_FUNC_OBJECT_TYPE", "IL_SYS_NODE_TYPE")
  }

  def getExtData(identifier: String, mimeType: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, config: PublishConfig): Option[ObjectExtData]

  def getHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, config: PublishConfig): Option[Map[String, AnyRef]]

  def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]

  def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]

}
