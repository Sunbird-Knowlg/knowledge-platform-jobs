package org.sunbird.job.functions

import java.lang.reflect.Type
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util

import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.{PostPublishProcessorConfig, PostPublishProcessorStreamTask}
import org.sunbird.job.util.{CassandraUtil, HttpUtil}

class BatchCreateFunction(config: PostPublishProcessorConfig)
                         (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[BatchCreateFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  lazy private val mapper: ObjectMapper = new ObjectMapper()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(eData: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    println("Batch Create eData: " + eData)
    val startDate = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val request = new java.util.HashMap[String, AnyRef]() {{
      put("request", new java.util.HashMap[String, AnyRef]() {{
        put("courseId", eData.get("identifier"))
        put("name", eData.get("name"))
        if (eData.containsKey("createdBy"))
          put("createdBy", eData.get("createdBy"))
        if (eData.containsKey("createdFor"))
          put("createdFor", eData.get("createdFor"))
        put("enrollmentType", "open")
        put("startDate", startDate)
      }})
    }}
    val httpRequest = mapper.writeValueAsString(request)
    println("HTTP Request: " + httpRequest)
    val httpResponse = PostPublishProcessorStreamTask.httpUtil.post(config.lmsBaseUrl + "/private/v1/course/batch/create", httpRequest)
    if (httpResponse.status == 200) {
      logger.info("Batch create success: " + httpResponse.body)
    } else {
      logger.error("Batch create failed: " + httpResponse.status + " :: " + httpResponse.body)
    }
  }

  override def metricsList(): List[String] = {
    List()
  }

}
