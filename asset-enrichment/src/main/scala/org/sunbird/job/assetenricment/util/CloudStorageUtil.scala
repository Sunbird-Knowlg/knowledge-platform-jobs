package org.sunbird.job.assetenricment.util

import org.apache.commons.lang3.StringUtils
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.sunbird.job.assetenricment.task.AssetEnrichmentConfig

import java.io.File

class CloudStorageUtil(config: AssetEnrichmentConfig) extends Serializable {

  val cloudStorageType: String = config.getString("cloud_storage_type", "azure")
  val storageService: BaseStorageService = getService
  val container: String = getContainerName

  def getService: BaseStorageService = {
    if (StringUtils.equalsIgnoreCase(cloudStorageType, "azure")) {
      val azureStorageKey = config.getString("azure_storage_key", "")
      val azureStorageSecret = config.getString("azure_storage_secret", "")
      StorageServiceFactory.getStorageService(StorageConfig(cloudStorageType, azureStorageKey, azureStorageSecret))
    } else if (StringUtils.equalsIgnoreCase(cloudStorageType, "aws")) {
      val awsStorageKey = config.getString("aws_storage_key", "")
      val awsStorageSecret = config.getString("aws_storage_secret", "")
      StorageServiceFactory.getStorageService(StorageConfig(cloudStorageType, awsStorageKey, awsStorageSecret))
    } else if (StringUtils.equalsIgnoreCase(cloudStorageType, "cephs3")) {
      val storageKey = config.getString("cephs3_storage_key", "");
      val storageSecret = config.getString("cephs3_storage_secret", "");
      val endPoint = config.getString("cephs3_storage_endpoint", "");
      StorageServiceFactory.getStorageService(StorageConfig(cloudStorageType, storageKey, storageSecret, Option(endPoint)))
    } else throw new Exception("Error while initialising cloud storage")
  }

  def getContainerName: String = {
    cloudStorageType match {
      case "azure" => config.getString("azure_storage_container", "")
      case "aws" => config.getString("aws_storage_container", "")
      case "cephs3" => config.getString("cephs3_storage_container", "")
      case _ => throw new Exception("Container name not configured.")
    }
  }

  def uploadFile(folderName: String, file: File, slug: Option[Boolean] = Option(true)): Array[String] = {
    val slugFile = if (slug.getOrElse(true)) Slug.createSlugFile(file) else file
    val objectKey = folderName + "/" + slugFile.getName
    val url = storageService.upload(container, slugFile.getAbsolutePath, objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty)
    Array[String](objectKey, url)
  }

  def copyObjectsByPrefix(sourcePrefix: String, destinationPrefix: String, isFolder: Boolean): Unit = {
    storageService.copyObjects(container, sourcePrefix, container, destinationPrefix, Option.apply(isFolder))
  }

}
