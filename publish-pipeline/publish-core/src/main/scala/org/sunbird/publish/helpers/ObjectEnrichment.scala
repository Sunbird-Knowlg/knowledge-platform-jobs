package org.sunbird.publish.helpers

import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}
import org.sunbird.publish.util.CloudStorageUtil

trait ObjectEnrichment extends FrameworkDataEnrichment with ThumbnailGenerator {


  def enrichObject(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil): ObjectData = {
    val newObj = enrichFrameworkData(obj)
    val enObj = enrichObjectMetadata(newObj).getOrElse(newObj)
    generateThumbnail(enObj).getOrElse(enObj)
  }

  def enrichObjectMetadata(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): Option[ObjectData]

}
