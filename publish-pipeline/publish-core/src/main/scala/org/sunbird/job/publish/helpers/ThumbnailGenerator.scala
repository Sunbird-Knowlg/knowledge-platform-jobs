package org.sunbird.job.publish.helpers

import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.util.CloudStorageUtil

trait ThumbnailGenerator {

  def generateThumbnail(obj: ObjectData)(implicit cloudStorageUtil: CloudStorageUtil): Option[ObjectData] = {
    Some(obj)
  }


}
