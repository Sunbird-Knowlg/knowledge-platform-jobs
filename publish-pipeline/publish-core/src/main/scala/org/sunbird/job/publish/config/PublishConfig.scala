package org.sunbird.job.publish.config

import com.typesafe.config.Config
import org.sunbird.job.BaseJobConfig

class PublishConfig(override val config: Config, override val jobName: String) extends BaseJobConfig(config, jobName) {

	def getString(key: String, default: String): String = {
		if(config.hasPath(key)) config.getString(key) else default
	}

	def getBoolean(key: String, default: Boolean): Boolean = {
		if(config.hasPath(key)) config.getBoolean(key) else default
	}

	def getConfig() = config

}
