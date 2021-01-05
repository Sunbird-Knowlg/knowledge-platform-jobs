package org.sunbird.publish.helpers

import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.core.ObjectData

import scala.collection.JavaConverters._

trait ObjectReader {

  def getObject(identifier: String)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil): ObjectData = {
    val metadata = getMetadata(identifier)
    val extData = getExtData(identifier)
    val hierarchy = getHierarchy(identifier)
    ObjectData(metadata, extData, hierarchy)
  }

  private def getMetadata(identifier: String)(implicit neo4JUtil: Neo4JUtil): Map[String, AnyRef] = {
    neo4JUtil.getNodeProperties(identifier).asScala.toMap
  }

  def getExtData(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
    None
  }

  def getHierarchy(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
    None
  }
}
