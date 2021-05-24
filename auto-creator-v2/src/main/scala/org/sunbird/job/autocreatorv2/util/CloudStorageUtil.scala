package org.sunbird.job.autocreatorv2.util

import org.apache.commons.lang3.StringUtils
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.sunbird.job.task.AutoCreatorV2Config

import java.io.File

class CloudStorageUtil(config: AutoCreatorV2Config) extends Serializable {

	var storageService: BaseStorageService = null
	val cloudStorageType = config.getString("cloud_storage_type", "azure")
	val azureStorageContainer = config.getString("azure_storage_container", "")
	val awsStorageContainer = config.getString("aws_storage_container", "")

	@throws[Exception]
	def getService: BaseStorageService = {
		if (null == storageService) {
			if (StringUtils.equalsIgnoreCase(cloudStorageType, "azure")) {
				val azureStorageKey = config.getString("azure_storage_key", "")
				val azureStorageSecret = config.getString("azure_storage_secret", "")
				storageService = StorageServiceFactory.getStorageService(StorageConfig(cloudStorageType, azureStorageKey, azureStorageSecret))
			} else if (StringUtils.equalsIgnoreCase(cloudStorageType, "aws")) {
				val awsStorageKey = config.getString("aws_storage_key", "")
				val awsStorageSecret = config.getString("aws_storage_secret", "")
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

}
