package org.sunbird.user.feeds

import com.typesafe.config.Config
import org.sunbird.job.BaseJobConfig

class UserFeedConfig(override val config: Config) extends BaseJobConfig(config, "user-feeds") {

}
