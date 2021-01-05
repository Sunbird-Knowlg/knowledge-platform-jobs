package org.sunbird.publish.helpers

import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.core.ObjectData

import scala.collection.JavaConverters._

trait ObjectReader {

  def getObject(identifier: String, pkgVersion: Int)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil): ObjectData = {
    val metadata = getMetadata(identifier, pkgVersion)
    val extData = getExtData(identifier)
    val hierarchy = getHierarchy(identifier)
    ObjectData(identifier, metadata, extData, hierarchy)
  }

  private def getMetadata(identifier: String, pkgVersion: Int)(implicit neo4JUtil: Neo4JUtil): Map[String, AnyRef] = {
    val nodeId = if (pkgVersion > 0) identifier + ".img" else identifier
    neo4JUtil.getNodeProperties(nodeId).asScala.toMap
  }

  def getExtData(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]

  def getHierarchy(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]
}
