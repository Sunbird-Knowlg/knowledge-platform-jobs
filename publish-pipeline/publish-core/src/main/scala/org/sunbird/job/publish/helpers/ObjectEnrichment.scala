package org.sunbird.job.publish.helpers

import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, JanusGraphUtil}

trait ObjectEnrichment extends FrameworkDataEnrichment with ThumbnailGenerator {

  def enrichObject(obj: ObjectData)(implicit janusGraphUtil: JanusGraphUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig): ObjectData = {
    val newObj = enrichFrameworkData(obj)
    val enObj = enrichObjectMetadata(newObj).getOrElse(newObj)
    generateThumbnail(enObj).getOrElse(enObj)
  }

  def enrichObjectMetadata(obj: ObjectData)(implicit janusGraphUtil: JanusGraphUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig): Option[ObjectData]

}
