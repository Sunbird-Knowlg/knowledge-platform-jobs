package org.sunbird.job.assetenricment.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.assetenricment.models.Asset
import org.sunbird.job.assetenricment.util.CloudStorageUtil

import java.io.File

trait OptimizerHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[OptimizerHelper])

  def replaceArtifactUrl(asset: Asset)(cloudStorageUtil: CloudStorageUtil): Unit = {
    logger.info(s"Replacing ArtifactUrl for Asset Id : ${asset.identifier}")
    val sourcePath = asset.artifactUrl.substring(asset.artifactUrl.indexOf(asset.artifactBasePath))
    val destinationPath = sourcePath.replace(asset.artifactBasePath + File.separator, "")
    try {
      cloudStorageUtil.copyObjectsByPrefix(sourcePath, destinationPath, false)
      logger.info(s"Copying Objects...DONE | Under: ${destinationPath} for identifier : ${asset.identifier}")
      val newArtifactUrl = asset.artifactUrl.replace(sourcePath, destinationPath)
      asset.put("artifactUrl", newArtifactUrl)
      asset.put("downloadUrl", newArtifactUrl)
      val storageKey = asset.get("cloudStorageKey", "").asInstanceOf[String]
      if (storageKey.nonEmpty) {
        val cloudStorageKey = storageKey.replace(asset.artifactBasePath + File.separator, "")
        asset.put("cloudStorageKey", cloudStorageKey)
      }
      val s3Key = asset.get("s3Key", "").asInstanceOf[String]
      if (s3Key.nonEmpty) {
        val s3KeyNew = s3Key.replace(asset.artifactBasePath + File.separator, "")
        asset.put("s3Key", s3KeyNew)
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error while copying object by prefix for identifier : ${asset.identifier}", e)
        throw e
    }
  }

}
