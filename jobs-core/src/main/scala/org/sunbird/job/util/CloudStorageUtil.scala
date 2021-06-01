package org.sunbird.job.util

import java.io.{File, FileOutputStream, IOException}
import java.net.URL
import java.nio.channels.Channels

import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

class CloudStorageUtil(cloudStoreType: String, storageKey: Option[String], storageSecret: Option[String], retryCount: Option[Int] = Option(3)) extends Serializable {

  val storageService: BaseStorageService = StorageServiceFactory.getStorageService(StorageConfig(cloudStoreType, storageKey.get, storageSecret.get))

  def uploadFile(container: String, path: String, file: File, isDirectory: Boolean): String = {
    val objectKey = path + file.getName
    val url = storageService.upload(container, file.getAbsolutePath, objectKey, Option.apply(isDirectory), Option.apply(1), Option.apply(retryCount.get), Option.empty)
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
