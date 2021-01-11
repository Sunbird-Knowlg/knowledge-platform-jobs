package org.sunbird.publish.config

import com.typesafe.config.Config
import org.sunbird.job.BaseJobConfig

class PublishConfig(override val config: Config, override val jobName: String) extends BaseJobConfig(config, jobName) {

	def getString(key: String, default: String): String = {
		if(config.hasPath(key)) config.getString(key) else default
	}

	def getConfig() = config

	// Cloud Storage Config
	val cloudStorageType = config.getString("cloud_storage_type")
	val azureStorageKey = config.getString("azure_storage_key")
	val azureStorageSecret = config.getString("azure_storage_secret")
	val azureStorageContainer = config.getString("azure_storage_container")
	val awsStorageKey = config.getString("aws_storage_key")
	val awsStorageSecret = config.getString("aws_storage_secret")
	val awsStorageContainer = config.getString("aws_storage_container")


}
