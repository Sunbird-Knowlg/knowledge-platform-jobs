package org.sunbird.job.helpers

import java.io.File

import org.slf4j.LoggerFactory
import org.sunbird.job.models.Asset
import org.sunbird.job.util.CloudStorageUtil

trait OptimizerHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[OptimizerHelper])

  def replaceArtifactUrl(asset: Asset)(cloudStorageUtil: CloudStorageUtil): Unit = {
    if (asset.artifactUrl.contains(asset.artifactBasePath)) {
      val sourcePath = asset.artifactUrl.replace(asset.artifactBasePath + File.separator, "")
      val destinationPath = sourcePath.replace(asset.artifactBasePath + File.separator, "")
      try {
        cloudStorageUtil.copyObjectsByPrefix(sourcePath, destinationPath, false)
        logger.info(s"Copying Objects...DONE | Under: ${destinationPath} for identifier : ${asset.identifier}")
        val newArtifactUrl = asset.artifactUrl.replace(sourcePath, destinationPath)
        asset.addToMetaData("artifactUrl", newArtifactUrl)
        asset.addToMetaData("downloadUrl", newArtifactUrl)
        val storageKey = asset.getFromMetaData("cloudStorageKey", "").asInstanceOf[String]
        if (storageKey.nonEmpty) {
          val cloudStorageKey = storageKey.replace(asset.artifactBasePath + File.separator, "")
          asset.addToMetaData("cloudStorageKey", cloudStorageKey)
        }
        val s3Key = asset.getFromMetaData("s3Key", "").asInstanceOf[String]
        if (s3Key.nonEmpty) {
          val s3KeyNew = s3Key.replace(asset.artifactBasePath + File.separator, "")
          asset.addToMetaData("s3Key", s3KeyNew)
        }
      } catch {
        case e: Exception =>
          logger.error(s"Error while copying object by prefix for identifier : ${asset.identifier}", e)
          throw e
      }
    }
  }

  def validateForArtifactUrl(asset: Asset, contentUploadContextDriven: Boolean): Boolean = {
    contentUploadContextDriven && !asset.artifactBasePath.isEmpty && !asset.artifactUrl.isEmpty
  }

}
