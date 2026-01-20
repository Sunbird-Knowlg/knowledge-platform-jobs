package org.sunbird.job.util

import org.apache.commons.lang3.StringUtils
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.sunbird.job.BaseJobConfig

import java.io.File

class CloudStorageUtil(config: BaseJobConfig) extends Serializable {

  val cloudStorageType: String = config.getString("cloud_storage_type", "azure")
  var storageService: BaseStorageService = null
  val container: String = getContainerName

  @throws[Exception]
  def getService: BaseStorageService = {
    if (null == storageService) {
      val storageKey = config.getString("cloud_storage_key", "")
      val storageSecret = config.getString("cloud_storage_secret", "")
      val endPoint = config.getString("cloud_storage_endpoint", "")
      val storageRegion = config.getString("cloud_storage_region", "ap-south-1")
      val authType = config.getString("cloud_storage_auth_type", "access_key")
      val storageEndPoint = if (StringUtils.isNotBlank(endPoint)) Option(endPoint) else None
      val storageAuthType = Option(authType)
      storageService = StorageServiceFactory.getStorageService(StorageConfig(cloudStorageType, storageKey, storageSecret, storageEndPoint, storageRegion, storageAuthType))
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
    val slugFile = if (slug.getOrElse(true)) Slug.createSlugFile(file) else file
    val objectKey = folderName + "/" + slugFile.getName
    val url = getService.upload(container, slugFile.getAbsolutePath, objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty)
    Array[String](objectKey, url)
  }

  def copyObjectsByPrefix(sourcePrefix: String, destinationPrefix: String, isFolder: Boolean): Unit = {
    getService.copyObjects(container, sourcePrefix, container, destinationPrefix, Option.apply(isFolder))
  }

  def getURI(prefix: String, isDirectory: Option[Boolean]): String = {
    getService.getUri(getContainerName, prefix, isDirectory)
  }

  def uploadDirectory(folderName: String, directory: File, slug: Option[Boolean] = Option(true)): Array[String] = {
    val slugFile = if (slug.getOrElse(true)) Slug.createSlugFile(directory) else directory
    val objectKey = folderName + File.separator
    val url = getService.upload(getContainerName, slugFile.getAbsolutePath, objectKey, Option.apply(true), Option.apply(1), Option.apply(5), Option.empty)
    Array[String](objectKey, url)
  }

  def deleteFile(key: String, isDirectory: Option[Boolean] = Option(false)): Unit = {
    getService.deleteObject(getContainerName, key, isDirectory)
  }

  def downloadFile(downloadPath: String, file: String, slug: Option[Boolean] = Option(false)): Unit = {
    getService.download(getContainerName, file, downloadPath, slug)
  }

}
