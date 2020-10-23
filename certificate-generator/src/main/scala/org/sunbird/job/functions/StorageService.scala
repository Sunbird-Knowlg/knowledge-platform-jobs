package org.sunbird.job.functions

import java.io.File
import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.sunbird.incredible.UrlManager
import org.sunbird.job.Exceptions.ServerException
import org.sunbird.job.task.CertificateGeneratorConfig


class StorageService extends Serializable {
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  var storageService: BaseStorageService = _

  @throws[Exception]
  def getService(config: CertificateGeneratorConfig): BaseStorageService = {
    val storageType: String = config.storageType
    if (null == storageService) {
      if (StringUtils.equalsIgnoreCase(storageType, "azure")) {
        val storageKey = config.azureStorageKey
        val storageSecret = config.azureStorageSecret
        storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, storageKey, storageSecret))
      } else if (StringUtils.equalsIgnoreCase(storageType, "aws")) {
        val storageKey = config.awsStorageKey
        val storageSecret = config.azureStorageSecret
        storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, storageKey, storageSecret))
      } else throw ServerException("ERR_INVALID_CLOUD_STORAGE", "Error while initialising cloud storage")
    }
    storageService
  }

  def uploadFile(config: CertificateGeneratorConfig, file: File, path: String, isDirectory: Boolean): String = {
    val objectKey = path + file.getName
    val url = getService(config).upload(config.containerName, file.getAbsolutePath, objectKey, Option.apply(isDirectory), Option.apply(1), Option.apply(5), Option.empty)
    UrlManager.getSharableUrl(url, config.containerName)
  }

  def downloadFile(config: CertificateGeneratorConfig, container: String, fileName: String, localPath: String, isDirectory: Boolean): Unit = {
    getService(config).download(container, fileName, localPath, Option.apply(isDirectory))
  }

}

