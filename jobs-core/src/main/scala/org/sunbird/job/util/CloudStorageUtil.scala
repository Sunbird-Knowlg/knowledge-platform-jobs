package org.sunbird.job.util

import org.apache.commons.lang3.StringUtils
import org.sunbird.cloud.storage.{IStorageService, StorageConfig, StorageServiceFactory}
import org.sunbird.cloud.storage.StorageConfig.StorageType
import org.sunbird.job.BaseJobConfig

import java.io.File

class CloudStorageUtil(config: BaseJobConfig) extends Serializable {

  val cloudStorageType: String = config.getString("cloud_storage_type", "azure")
  var storageService: IStorageService = null
  val container: String = getContainerName

  @throws[Exception]
  def getService: IStorageService = {
    if (null == storageService) {
      val storageKey    = config.getString("cloud_storage_key", "")
      val storageSecret = config.getString("cloud_storage_secret", "")
      val endPoint      = config.getString("cloud_storage_endpoint", "")
      val region        = config.getString("cloud_storage_region", "")
      val authTypeStr   = config.getString("cloud_storage_auth_type", "ACCESS_KEY").toUpperCase

      val storageType = cloudStorageType.toLowerCase match {
        case "aws"    => StorageType.AWS
        case "azure"  => StorageType.AZURE
        case "gcloud" => StorageType.GCLOUD
        case "oci"    => StorageType.OCI
        case "cephs3" => StorageType.CEPHS3
        case other    => throw new Exception(s"Unsupported cloud storage type: $other")
      }

      val authType = StorageConfig.AuthType.valueOf(authTypeStr)

      val builder = StorageConfig.builder(storageType)
        .storageKey(storageKey)
        .authType(authType)

      // For ACCESS_KEY auth: provide the static secret from configuration.
      // For OIDC / IAM / IAM_ROLE / INSTANCE_PROFILE: the cloud SDK resolves credentials
      // automatically via Workload Identity, Managed Identity, or the default credential chain —
      // no static secret is needed (and setting it may confuse some providers).
      if (authType == StorageConfig.AuthType.ACCESS_KEY) {
        builder.storageSecret(storageSecret)
      }

      if (StringUtils.isNotBlank(endPoint)) builder.endPoint(endPoint)
      if (StringUtils.isNotBlank(region))   builder.region(region)

      storageService = StorageServiceFactory.getStorageService(builder.build())
    }
    storageService
  }

  def getContainerName: String = {
    if (StringUtils.isBlank(config.getString("cloud_storage_container", "")))
      throw new Exception("Container name not configured.")
    else
      config.getString("cloud_storage_container", "")
  }

  def uploadFile(folderName: String, file: File, slug: Option[Boolean] = Option(true), container: String = container): Array[String] = {
    val slugFile  = if (slug.getOrElse(true)) Slug.createSlugFile(file) else file
    val objectKey = folderName + "/" + slugFile.getName
    val url       = getService.upload(container, slugFile.getAbsolutePath, objectKey, false, 1, 5, null)
    Array[String](objectKey, url)
  }

  def copyObjectsByPrefix(sourcePrefix: String, destinationPrefix: String, isFolder: Boolean): Unit = {
    getService.copyObjects(container, sourcePrefix, container, destinationPrefix, isFolder)
  }

  def getURI(prefix: String, isDirectory: Option[Boolean]): String = {
    getService.getUri(getContainerName, prefix, isDirectory.getOrElse(false))
  }

  def uploadDirectory(folderName: String, directory: File, slug: Option[Boolean] = Option(true)): Array[String] = {
    val slugFile  = if (slug.getOrElse(true)) Slug.createSlugFile(directory) else directory
    val objectKey = folderName + File.separator
    val url       = getService.upload(getContainerName, slugFile.getAbsolutePath, objectKey, true, 1, 5, null)
    Array[String](objectKey, url)
  }

  def deleteFile(key: String, isDirectory: Option[Boolean] = Option(false)): Unit = {
    getService.deleteObject(getContainerName, key, isDirectory.getOrElse(false))
  }

  def downloadFile(downloadPath: String, file: String, slug: Option[Boolean] = Option(false)): Unit = {
    getService.download(getContainerName, file, downloadPath, slug.getOrElse(false))
  }

}
