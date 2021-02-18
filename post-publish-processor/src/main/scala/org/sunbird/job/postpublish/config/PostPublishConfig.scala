package org.sunbird.job.postpublish.config

import com.typesafe.config.Config
import org.sunbird.job.BaseJobConfig

class PostPublishConfig(override val config: Config, override val jobName: String) extends BaseJobConfig(config, jobName) {

    def getString(key: String, default: String): String = {
        if(config.hasPath(key)) config.getString(key) else default
    }

    def getConfig() = config

}