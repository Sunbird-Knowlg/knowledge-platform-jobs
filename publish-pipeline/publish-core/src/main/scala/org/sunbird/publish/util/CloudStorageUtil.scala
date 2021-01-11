package org.sunbird.publish.util

import java.io.File

import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.sunbird.publish.config.PublishConfig
import org.sunbird.publish.core.Slug

import scala.concurrent.ExecutionContext

class CloudStorageUtil(config: PublishConfig) {

	var storageService: BaseStorageService = null
	val cloudStorageType = config.getString("cloud_storage_type", "azure")
	val azureStorageKey = config.getString("azure_storage_key", "")
	val azureStorageSecret = config.getString("azure_storage_secret", "")
	val azureStorageContainer = config.getString("azure_storage_container", "")
	val awsStorageKey = config.getString("aws_storage_key", "")
	val awsStorageSecret = config.getString("aws_storage_secret", "")
	val awsStorageContainer = config.getString("aws_storage_container", "")

	@throws[Exception]
	def getService(): BaseStorageService = {
		if (null == storageService) {
			cloudStorageType match {
				case "azure" => storageService = StorageServiceFactory.getStorageService(StorageConfig(cloudStorageType, azureStorageKey, azureStorageSecret))
				case "aws" => storageService = StorageServiceFactory.getStorageService(StorageConfig(awsStorageKey, awsStorageSecret, awsStorageContainer))
				case _ => throw new Exception("Error while initialising cloud storage")
			}
		}
		storageService
	}

	def getContainerName(): String = {
		cloudStorageType match {
			case "azure" => azureStorageContainer
			case "aws" => awsStorageContainer
			case _ => throw new Exception("Container name not configured.")
		}
	}

	def uploadFile(folderName: String, file: File, slug: Option[Boolean] = Option(true)): Array[String] = {
		val slugFile = if (slug.getOrElse(true)) Slug.createSlugFile(file) else file
		val objectKey = folderName + "/" + slugFile.getName
		val url = getService.upload(getContainerName, slugFile.getAbsolutePath, objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty)
		Array[String](objectKey, url)
	}

	def uploadDirectory(folderName: String, directory: File, slug: Option[Boolean] = Option(true)): Array[String] = {
		val slugFile = if (slug.getOrElse(true)) Slug.createSlugFile(directory) else directory
		val objectKey = folderName + File.separator
		val url = getService.upload(getContainerName(), slugFile.getAbsolutePath, objectKey, Option.apply(true), Option.apply(1), Option.apply(5), Option.empty)
		Array[String](objectKey, url)
	}

	def uploadDirectoryAsync(folderName: String, directory: File, slug: Option[Boolean] = Option(true))(implicit ec: ExecutionContext) = {
		val slugFile = if (slug.getOrElse(true)) Slug.createSlugFile(directory) else directory
		val objectKey = folderName + File.separator
		getService.uploadFolder(getContainerName, slugFile.getAbsolutePath, objectKey, Option.apply(false), None, None, 1)
	}

	def getObjectSize(key: String): Double = {
		val blob = getService.getObject(getContainerName, key, Option.apply(false))
		blob.contentLength
	}

	def copyObjectsByPrefix(source: String, destination: String) = {
		getService.copyObjects(getContainerName, source, getContainerName, destination, Option.apply(true))
	}

	def deleteFile(key: String, isDirectory: Option[Boolean] = Option(false)) = {
		getService.deleteObject(getContainerName, key, isDirectory)
	}

	def getSignedURL(key: String, ttl: Option[Int], permission: Option[String]): String = {
		getService().getSignedURL(getContainerName, key, ttl, permission)
	}

}
