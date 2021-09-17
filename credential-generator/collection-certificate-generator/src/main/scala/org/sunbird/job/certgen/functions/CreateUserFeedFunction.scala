package org.sunbird.job.certgen.functions

import java.util.Date
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.certgen.task.CertificateGeneratorConfig
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.util.{HTTPResponse, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

case class UserFeedMetaData(userId: String, courseName: String, issuedOn: Date, courseId: String, partition: Int, offset: Long)

class CreateUserFeedFunction(config: CertificateGeneratorConfig, httpUtil: HttpUtil)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[UserFeedMetaData, String](config) {

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
    val req = {
      s"""{"request":{"notifications":[{"ids":["${metaData.userId}"],"priority":1,"type":"feed","action":{"type":"certificateUpdate","category":"Notification","template":{"type":"JSON","params":{}},"createdBy":{"id":"certificate-generator","type":"System"},"additionalInfo":{"actionType":"certificateUpdate","title":"${metaData.courseName}","description":"${config.userFeedMsg}","identifier":"${metaData.courseId}"}}}]}}"""
    }
    val url = config.notificationServiceBaseUrl + config.userFeedCreateEndPoint
    val response: HTTPResponse = httpUtil.post(url, req)
    if (response.status == 200) {
      metrics.incCounter(config.userFeedCount)
      logger.info("user feed response status {} :: {}", response.status, response.body)
    }
    else {
      metrics.incCounter(config.failedEventCount)
      logger.error(s"Error response from createUserFeed API for request :: ${req} :: response is :: ${response.status} ::  ${response.body}")
      throw new InvalidEventException(s"Error in UserFeed Response: ${response}", Map("partition" -> metaData.partition, "offset" -> metaData.offset), null)
    }
  }

  override def metricsList(): List[String] = {
    List(config.userFeedCount, config.failedEventCount)
  }
}
