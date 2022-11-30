package org.sunbird.job.livenodepublisher.publish.processor

import org.sunbird.job.util.CloudStorageUtil

class BaseProcessor(basePath: String, identifier: String)(implicit cloudStorageUtil: CloudStorageUtil) extends IProcessor(basePath, identifier) {
  override def process(ecrf: Plugin): Plugin = {
    ecrf
  }
}
