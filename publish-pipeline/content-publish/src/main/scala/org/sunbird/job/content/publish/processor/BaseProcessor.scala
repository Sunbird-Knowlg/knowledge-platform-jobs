package org.sunbird.job.content.publish.processor

import org.sunbird.job.publish.util.CloudStorageUtil

class BaseProcessor(basePath: String, identifier: String)(implicit cloudStorageUtil: CloudStorageUtil) extends IProcessor(basePath, identifier) {
    override def process(ecrf: Plugin): Plugin = {
        ecrf
    }
}
