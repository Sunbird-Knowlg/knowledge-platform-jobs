package org.sunbird.spec

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.domain.reader.JobRequest

import scala.collection.mutable.Map

class BaseProcessTestConfig(override val config: Config) extends BaseJobConfig(config, "Test-job") {
  private val serialVersionUID = -2349318979085017498L
  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val jobReqTypeInfo: TypeInformation[TestJobRequest] = TypeExtractor.getForClass(classOf[TestJobRequest])

  val mapOutputTag: OutputTag[Map[String, AnyRef]] = OutputTag[Map[String, AnyRef]]("test-map-stream-tag")
  val stringOutputTag: OutputTag[String] = OutputTag[String]("test-string-stream-tag")
  val jobRequestOutputTag: OutputTag[TestJobRequest] = OutputTag[TestJobRequest]("test-job-request-stream-tag")

  val kafkaMapInputTopic: String = config.getString("kafka.map.input.topic")
  val kafkaMapOutputTopic: String = config.getString("kafka.map.output.topic")
  val kafkaStringInputTopic: String = config.getString("kafka.string.input.topic")
  val kafkaStringOutputTopic: String = config.getString("kafka.string.output.topic")
  val kafkaJobReqInputTopic: String = config.getString("kafka.jobRequest.input.topic")
  val kafkaJobReqOutputTopic: String = config.getString("kafka.jobRequest.output.topic")

  val testTopics = List(kafkaMapInputTopic, kafkaMapOutputTopic,
    kafkaStringInputTopic, kafkaStringOutputTopic, kafkaJobReqInputTopic, kafkaJobReqOutputTopic)

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  val cacheExpirySeconds: Int = config.getInt("redis.database.key.expiry.seconds")
  val mapEventCount = "map-event-count"
  val telemetryEventCount = "telemetry-event-count"
  val stringEventCount = "string-event-count"
  val jobRequestEventCount = "job-request-event-count"

}