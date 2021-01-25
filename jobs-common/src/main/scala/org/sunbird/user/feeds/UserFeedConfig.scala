package org.sunbird.user.feeds

import com.typesafe.config.Config
import org.sunbird.job.BaseJobConfig

class UserFeedConfig(override val config: Config) extends BaseJobConfig(config, "user-feed") {


  val learnerServiceBaseUrl: String = config.getString("learner-service.basePath")
  val userFeedCreateEndPoint:String = "/private/user/feed/v1/create"

  val l1: String = "l1"
  val id: String = "id"
  val data: String = "data"
  val category: String = "category"
  val certificates: String = "certificates"
  val priority: String = "priority"
  val userFeedMsg: String = "You have earned a certificate! Download it from your profile page."
  val priorityValue = 1

  val userFeedCount = "user-feed-count"

}
