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
      val storageConfig = CloudStorageUtil.buildStorageConfig(
        cloudStorageType,
        config.getString("cloud_storage_key", ""),
        config.getString("cloud_storage_secret", ""),
        config.getString("cloud_storage_auth_type", "ACCESS_KEY").toUpperCase,
        config.getString("cloud_storage_endpoint", ""),
        config.getString("cloud_storage_region", "")
      )
      storageService = StorageServiceFactory.getStorageService(storageConfig)
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

object CloudStorageUtil {

  /**
   * Pure function: maps raw config strings to a [[StorageConfig]] ready to hand to
   * [[StorageServiceFactory]].  All branching logic (type mapping, auth-type parsing,
   * conditional secret / endpoint / region injection) lives here so it can be unit-tested
   * without touching any cloud SDK network calls.
   *
   * Throws [[IllegalArgumentException]] for an unrecognised `cloudStorageType` or an
   * invalid `authTypeStr` (the latter is delegated to [[StorageConfig.AuthType#valueOf]]).
   */
  private[util] def buildStorageConfig(
    cloudStorageType: String,
    storageKey: String,
    storageSecret: String,
    authTypeStr: String,
    endPoint: String,
    region: String
  ): StorageConfig = {

    val storageType = cloudStorageType.toLowerCase match {
      case "aws"    => StorageType.AWS
      case "azure"  => StorageType.AZURE
      case "gcloud" => StorageType.GCLOUD
      case "oci"    => StorageType.OCI
      case "cephs3" => StorageType.CEPHS3
      case other    => throw new IllegalArgumentException(s"Unsupported cloud storage type: $other")
    }

    // valueOf throws IllegalArgumentException for unrecognised names — intentionally propagated.
    val authType = StorageConfig.AuthType.valueOf(authTypeStr)

    val builder = StorageConfig.builder(storageType)
      .storageKey(storageKey)
      .authType(authType)

    // For ACCESS_KEY auth: supply the static secret from configuration.
    // For OIDC / IAM / IAM_ROLE / INSTANCE_PROFILE: the cloud SDK resolves credentials
    // automatically via Workload Identity, Managed Identity, or the default credential chain —
    // no static secret is needed (and providing one may confuse some providers).
    if (authType == StorageConfig.AuthType.ACCESS_KEY) builder.storageSecret(storageSecret)

    if (StringUtils.isNotBlank(endPoint)) builder.endPoint(endPoint)
    if (StringUtils.isNotBlank(region))   builder.region(region)

    builder.build()
  }

}
