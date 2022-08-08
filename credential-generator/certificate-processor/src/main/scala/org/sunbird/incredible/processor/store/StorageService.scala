package org.sunbird.incredible.processor.store

import java.io.File

import org.apache.commons.lang3.StringUtils
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.StorageConfig
import org.sunbird.cloud.storage.factory.StorageServiceFactory
import org.sunbird.incredible.pojos.exceptions.ServerException
import org.sunbird.incredible.{JsonKeys, StorageParams, UrlManager}


class StorageService(storageParams: StorageParams) extends Serializable {

  var storageService: BaseStorageService = _
  val storageType: String = storageParams.cloudStorageType

  @throws[Exception]
  def getService: BaseStorageService = {
    if (null == storageService) {
      if (StringUtils.equalsIgnoreCase(storageType, JsonKeys.AZURE)) {
        val storageKey = storageParams.azureStorageKey
        val storageSecret = storageParams.azureStorageSecret
        storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, storageKey, storageSecret))
      } else if (StringUtils.equalsIgnoreCase(storageType, JsonKeys.AWS)) {
        val storageKey = storageParams.awsStorageKey.get
        val storageSecret = storageParams.awsStorageSecret.get
        storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, storageKey, storageSecret))
      } else if (StringUtils.equalsIgnoreCase(storageType, JsonKeys.CEPHS3)) {
        val storageKey = storageParams.cephs3StorageKey.get
        val storageSecret = storageParams.cephs3StorageSecret.get
        val cephs3StorageEndPoint = storageParams.cephs3StorageEndPoint.get
        storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, storageKey, storageSecret,Option.apply(cephs3StorageEndPoint)))
      } else throw new ServerException("ERR_INVALID_CLOUD_STORAGE", "Error while initialising cloud storage")
    }
    storageService
  }

  def getContainerName: String = {
    if (StringUtils.equalsIgnoreCase(storageType, JsonKeys.AZURE))
      storageParams.azureContainerName
    else if (StringUtils.equalsIgnoreCase(storageType, JsonKeys.AWS))
      storageParams.awsContainerName.get
    else if (StringUtils.equalsIgnoreCase(storageType, JsonKeys.CEPHS3))
      storageParams.cephs3ContainerName.get
    else
      throw new ServerException("ERR_INVALID_CLOUD_STORAGE", "Container name not configured.")
  }

  def uploadFile(path: String, file: File): String = {
    val objectKey = path + file.getName
    val containerName = getContainerName
    val url = getService.upload(containerName, file.getAbsolutePath, objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty)
    UrlManager.getSharableUrl(url, containerName)
  }


}
