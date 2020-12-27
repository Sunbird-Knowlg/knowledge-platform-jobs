package org.sunbird.incredible.processor.store

import java.io.File

import org.apache.commons.lang3.StringUtils
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.StorageConfig
import org.sunbird.cloud.storage.factory.StorageServiceFactory
import org.sunbird.incredible.pojos.exceptions.ServerException
import org.sunbird.incredible.{JsonKeys, UrlManager}


class StorageService(storageParams: Map[String, String]) {

  var storageService: BaseStorageService = _
  val storageType: String = storageParams.getOrElse(JsonKeys.CLOUD_STORAGE_TYPE, "")

  @throws[Exception]
  def getService: BaseStorageService = {
    if (null == storageService) {
      if (StringUtils.equalsIgnoreCase(storageType, JsonKeys.AZURE)) {
        val storageKey = storageParams("azure_storage_key")
        val storageSecret = storageParams("azure_storage_secret")
        storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, storageKey, storageSecret))
      } else if (StringUtils.equalsIgnoreCase(storageType, JsonKeys.AWS)) {
        val storageKey = storageParams("aws_storage_key")
        val storageSecret = storageParams("aws_storage_secret")
        storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, storageKey, storageSecret))
      } else throw ServerException("ERR_INVALID_CLOUD_STORAGE", "Error while initialising cloud storage")
    }
    storageService
  }

  def getContainerName: String = {
    if (StringUtils.equalsIgnoreCase(storageType, JsonKeys.AZURE))
      storageParams(JsonKeys.CONTAINER_NAME)
    else if (StringUtils.equalsIgnoreCase(storageType, JsonKeys.AWS))
      storageParams("aws_storage_container")
    else
      throw ServerException("ERR_INVALID_CLOUD_STORAGE", "Container name not configured.")
  }

  def uploadFile(path: String, file: File, isDirectory: Boolean, retryCount: Int): String = {
    val objectKey = path + file.getName
    val containerName = getContainerName
    val url = getService.upload(containerName, file.getAbsolutePath, objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty)
    UrlManager.getSharableUrl(url, containerName)
  }


}
