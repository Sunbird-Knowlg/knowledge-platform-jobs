package org.sunbird.publish.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.core.ObjectData

import scala.collection.JavaConverters._

trait ObjectReader {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectReader])

  def getObject(identifier: String, pkgVersion: Double)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil): ObjectData = {
    logger.info("Reading editable object data for: " + identifier + " with pkgVersion: " + pkgVersion)
    val metadata = getMetadata(identifier, pkgVersion)
    val extData = getExtData(identifier)
    val hierarchy = getHierarchy(identifier)
    new ObjectData(identifier, metadata, extData, hierarchy)
  }

  private def getMetadata(identifier: String, pkgVersion: Double)(implicit neo4JUtil: Neo4JUtil): Map[String, AnyRef] = {
    val nodeId = getEditableObjId(identifier, pkgVersion)
    Option(neo4JUtil.getNodeProperties(nodeId)).getOrElse(neo4JUtil.getNodeProperties(identifier)).asScala.toMap
  }

  def getExtData(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]

  def getHierarchy(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]

  def getEditableObjId(identifier: String, pkgVersion: Double): String = {
    if (pkgVersion > 0) identifier + ".img" else identifier
  }
}
