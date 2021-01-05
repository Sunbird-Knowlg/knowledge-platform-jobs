package org.sunbird.notifier

import com.typesafe.config.Config
import org.sunbird.job.BaseJobConfig

class NotifierConfig(override val config: Config) extends BaseJobConfig(config, "notifier") {
  val userId: String = "userId"


  private val serialVersionUID = 2905979434303791379L
  
  
}
