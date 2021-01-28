package org.sunbird.publish.helpers

import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}

trait ObjectEnrichment extends FrameworkDataEnrichment {


  def enrichObject(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): ObjectData = {
    val newObj = enrichFrameworkData(obj)
    enrichObjectMetadata(newObj).getOrElse(newObj)
  }

  def enrichObjectMetadata(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): Option[ObjectData]

}
