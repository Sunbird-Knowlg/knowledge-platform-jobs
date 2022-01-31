package org.sunbird.job.publish.config

import com.typesafe.config.Config
import org.sunbird.job.BaseJobConfig

class PublishConfig(override val config: Config, override val jobName: String) extends BaseJobConfig(config, jobName) {

  def getConfig() = config

}
