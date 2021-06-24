package org.sunbird.job.publish.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.publish.core.{ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}

import scala.collection.JavaConverters._

trait ObjectReader {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectReader])

  def getObject(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil): ObjectData = {
    logger.info("Reading editable object data for: " + identifier + " with pkgVersion: " + pkgVersion)
    val metadata = getMetadata(identifier, pkgVersion)
    val extData = getExtData(identifier, pkgVersion, readerConfig)
    new ObjectData(identifier, metadata, extData.getOrElse(ObjectExtData()).data, extData.getOrElse(ObjectExtData()).hierarchy)
  }

  private def getMetadata(identifier: String, pkgVersion: Double)(implicit neo4JUtil: Neo4JUtil): Map[String, AnyRef] = {
    val nodeId = getEditableObjId(identifier, pkgVersion)
    val metaData = Option(neo4JUtil.getNodeProperties(nodeId)).getOrElse(neo4JUtil.getNodeProperties(identifier)).asScala.toMap
    val id = metaData.getOrElse("IL_UNIQUE_ID", identifier).asInstanceOf[String]
    val objType = metaData.getOrElse("IL_FUNC_OBJECT_TYPE", "").asInstanceOf[String]
    metaData ++ Map[String, AnyRef]("identifier" -> id, "objectType" -> objType) - ("IL_UNIQUE_ID", "IL_FUNC_OBJECT_TYPE", "IL_SYS_NODE_TYPE")
  }

  def getExtData(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[ObjectExtData]

  def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]

  def getEditableObjId(identifier: String, pkgVersion: Double): String = {
    if (pkgVersion > 0) identifier + ".img" else identifier
  }

  def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]

  def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]

}
