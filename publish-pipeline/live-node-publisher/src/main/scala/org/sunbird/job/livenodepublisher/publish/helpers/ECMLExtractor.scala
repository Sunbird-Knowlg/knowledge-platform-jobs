package org.sunbird.job.livenodepublisher.publish.helpers

import org.sunbird.job.livenodepublisher.publish.processor.{BaseProcessor, MissingAssetValidatorProcessor}
import org.sunbird.job.util.CloudStorageUtil

class ECMLExtractor(basePath: String, identifier: String)(implicit cloudStorageUtil: CloudStorageUtil) extends BaseProcessor(basePath, identifier) with MissingAssetValidatorProcessor {

}
