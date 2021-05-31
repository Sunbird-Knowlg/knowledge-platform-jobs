package org.sunbird.user.feeds

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.util.{HTTPResponse, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

case class UserFeedMetaData(userId: String, courseName: String, issuedOn: Date, courseId: String)

class CreateUserFeedFunction(config: UserFeedConfig, httpUtil: HttpUtil)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[UserFeedMetaData, String](config) {

  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
  private[this] val logger = LoggerFactory.getLogger(classOf[CreateUserFeedFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(metaData: UserFeedMetaData,
                              context: ProcessFunction[UserFeedMetaData, String]#Context,
                              metrics: Metrics): Unit = {
    val req = s"""{"request":{"userId":"${metaData.userId}","category":"Notification","priority":1,"data":{"type":1,"actionDtata":{"actionType":"certificateUpdate","title":"${metaData.courseName}","description":"${config.userFeedMsg}","identifier":"${metaData.courseId}"}}}}"""
    val url = config.learnerServiceBaseUrl + config.userFeedCreateEndPoint
    try {
      val response: HTTPResponse = httpUtil.post(url, req)
      if (response.status == 200) {
        metrics.incCounter(config.userFeedCount)
        logger.info("user feed response status {} :: {}", response.status, response.body)
      }
      else
        logger.info("user feed  response status {} :: {}", response.status, response.body)
    } catch {
      case e: Exception =>
        logger.error("Error while creating user feed : {}", metaData.userId + e)
    }
  }

  override def metricsList(): List[String] = {
    List(config.userFeedCount)
  }
}
