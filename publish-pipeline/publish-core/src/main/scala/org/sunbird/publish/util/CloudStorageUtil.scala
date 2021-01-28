package org.sunbird.publish.util

import java.io.File

import org.apache.commons.lang3.StringUtils
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.sunbird.publish.config.PublishConfig
import org.sunbird.publish.core.Slug

class CloudStorageUtil(config: PublishConfig) extends Serializable {

	var storageService: BaseStorageService = null
	val cloudStorageType = config.getString("cloud_storage_type", "azure")
	val azureStorageContainer = config.getString("azure_storage_container", "")
	val awsStorageContainer = config.getString("aws_storage_container", "")

	//@throws[Exception]
	/*def getService(): BaseStorageService = {

		if (null == storageService) {
			println("cloudStorageType :::: "+cloudStorageType)
			println("azureStorageKey :::: "+azureStorageKey)
			println("azureStorageSecret :::: "+azureStorageSecret)
			println("azureStorageContainer :::: "+azureStorageContainer)

			cloudStorageType match {
				case "azure" => storageService = StorageServiceFactory.getStorageService(StorageConfig(cloudStorageType, azureStorageKey, azureStorageSecret))
				case "aws" => storageService = StorageServiceFactory.getStorageService(StorageConfig(cloudStorageType, awsStorageKey, awsStorageSecret))
				case _ => throw new Exception("Error while initialising cloud storage")
			}
		}
		storageService
	}*/

	@throws[Exception]
	def getService: BaseStorageService = {
		if (null == storageService) {
			if (StringUtils.equalsIgnoreCase(cloudStorageType, "azure")) {
				val azureStorageKey = config.getString("azure_storage_key", "")
				val azureStorageSecret = config.getString("azure_storage_secret", "")
				//val storageKey = storageParams.azureStorageKey
				//val storageSecret = storageParams.azureStorageSecret
				storageService = StorageServiceFactory.getStorageService(StorageConfig(cloudStorageType, azureStorageKey, azureStorageSecret))
			} else if (StringUtils.equalsIgnoreCase(cloudStorageType, "aws")) {
				val awsStorageKey = config.getString("aws_storage_key", "")
				val awsStorageSecret = config.getString("aws_storage_secret", "")
				//val storageKey = storageParams.awsStorageKey.get
				//val storageSecret = storageParams.awsStorageSecret.get
				storageService = StorageServiceFactory.getStorageService(StorageConfig(cloudStorageType, awsStorageKey, awsStorageSecret))
			} else throw new Exception("Error while initialising cloud storage")
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
		getService.getSignedURL(getContainerName, key, ttl, permission)
	}

}
