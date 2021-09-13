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
    try {
      val req = s"""{"request":{"userId":"${
        metaData.userId
      }","category":"Notification","priority":1,"data":{"type":1,"actionData":{"actionType":"certificateUpdate","title":"${
        metaData.courseName
      }","description":"${config.userFeedMsg}","identifier":"${metaData.courseId}"}}}}"""
      val url = config.learnerServiceBaseUrl + config.userFeedCreateEndPoint
      val response: HTTPResponse = httpUtil.post(url, req)
      if (response.status == 200) {
        metrics.incCounter(config.userFeedCount)
        logger.info("user feed response status {} :: {}", response.status, response.body)
      }
      else
        throw new Exception(s"Exception is UserFeedResponse: ${response}")
    } catch {
      case e: Exception =>
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(e.getMessage, Map("partition" -> metaData.partition, "offset" -> metaData.offset), e)
    }
  }

  override def metricsList(): List[String] = {
    List(config.userFeedCount)
  }
}
