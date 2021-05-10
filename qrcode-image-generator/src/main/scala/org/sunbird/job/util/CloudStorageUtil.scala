package org.sunbird.job.util

import org.apache.commons.lang3.StringUtils
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.net.URL
import java.nio.channels.Channels

import org.sunbird.job.task.QRCodeImageGeneratorConfig


class CloudStorageUtil(config: QRCodeImageGeneratorConfig) extends Serializable {

  val cloudStoreType = config.cloudStorageType
  var storageService: BaseStorageService = getStorageService

  def getStorageService: BaseStorageService = {
    if (StringUtils.equalsIgnoreCase(cloudStoreType, "azure")) {
      val storageKey = config.azureStorageKey
      val storageSecret = config.azureStorageSecret
      StorageServiceFactory.getStorageService(StorageConfig(cloudStoreType, storageKey, storageSecret))
    }
    else if (StringUtils.equalsIgnoreCase(cloudStoreType, "aws")) {
      val storageKey = config.awsStorageKey
      val storageSecret = config.awsStorageSecret
      StorageServiceFactory.getStorageService(StorageConfig(cloudStoreType, storageKey, storageSecret))
    }
    else throw new Exception("Error while initialising cloud storage")
  }

  def uploadFile(container: String, path: String, file: File, isDirectory: Boolean): String = {
    val retryCount = config.cloudUploadRetryCount
    val objectKey = path + file.getName
    val url = storageService.upload(container, file.getAbsolutePath, objectKey, Option.apply(isDirectory), Option.apply(1), Option.apply(retryCount), Option.empty)
    url
  }

  @throws[IOException]
  def downloadFile(downloadUrl: String, fileToSave: File): Unit = {
    val url = new URL(downloadUrl)
    val readableByteChannel = Channels.newChannel(url.openStream)
    val fileOutputStream = new FileOutputStream(fileToSave)
    val fileChannel = fileOutputStream.getChannel
    fileOutputStream.getChannel.transferFrom(readableByteChannel, 0, java.lang.Long.MAX_VALUE)
    fileChannel.close()
    fileOutputStream.close()
    readableByteChannel.close()
  }
}

