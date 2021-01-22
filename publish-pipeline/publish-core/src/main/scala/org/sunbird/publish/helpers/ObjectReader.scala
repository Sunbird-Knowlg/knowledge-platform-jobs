package org.sunbird.publish.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}

import scala.collection.JavaConverters._

trait ObjectReader {

  private[this] val logger = LoggerFactory.getLogger(classOf[ObjectReader])

  def getObject(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil): ObjectData = {
    logger.info("Reading editable object data for: " + identifier + " with pkgVersion: " + pkgVersion)
    val metadata = getMetadata(identifier, pkgVersion)
    val extData = getExtData(identifier, readerConfig)
    val hierarchy = getHierarchy(identifier, readerConfig)
    new ObjectData(identifier, metadata, extData, hierarchy)
  }

  private def getMetadata(identifier: String, pkgVersion: Double)(implicit neo4JUtil: Neo4JUtil): Map[String, AnyRef] = {
    val nodeId = getEditableObjId(identifier, pkgVersion)
    Option(neo4JUtil.getNodeProperties(nodeId)).getOrElse(neo4JUtil.getNodeProperties(identifier)).asScala.toMap
  }

  def getExtData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]

  def getHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]

  def getEditableObjId(identifier: String, pkgVersion: Double): String = {
    if (pkgVersion > 0) identifier + ".img" else identifier
  }

    def getObjects(identifiers: List[String], readerConfig: ExtDataConfig)
                  (implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil): List[ObjectData] = {
        val metadataList = getMetadatas(identifiers)
        val extData = getExtDatas(identifiers, readerConfig)
        val hierarchy = getHierarchies(identifiers, readerConfig)
        identifiers.map(identifier => new ObjectData(identifier,
            metadataList.getOrElse(identifier, Map()).asInstanceOf[Map[String, AnyRef]],
            extData.getOrElse(Map()).get(identifier).asInstanceOf[Option[Map[String, AnyRef]]],
            hierarchy.getOrElse(Map()).get(identifier).asInstanceOf[Option[Map[String, AnyRef]]]))
    }

    private def getMetadatas(identifiers: List[String])(implicit neo4JUtil: Neo4JUtil): Map[String, AnyRef] = {
        Option(neo4JUtil.getNodesProps(identifiers)).getOrElse(Map())
    }

    def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]

    def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]]

}
