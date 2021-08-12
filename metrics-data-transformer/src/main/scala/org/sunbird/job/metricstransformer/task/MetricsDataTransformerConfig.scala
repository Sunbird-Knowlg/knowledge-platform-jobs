package org.sunbird.job.metricstransformer.task

import com.typesafe.config.Config
import org.sunbird.job.BaseJobConfig
import java.util.{List => JList}

class MetricsDataTransformerConfig(override val config: Config) extends BaseJobConfig(config, "metrics-data-transformer") {

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-events-count"

  val contentServiceBaseUrl : String = config.getString("service.content.basePath")
  val contentReadApi : String = config.getString("content_read_api")

  val lpURL: String = config.getString("service.sourcing.content.basePath")
  val contentUpdate = config.getString("content_update_api")

  val defaultHeaders = Map[String, String] ("Content-Type" -> "application/json")

  // Consumers
  val eventConsumer = "metrics-data-transformer-consumer"
  val metricsDataTransformerFunction = "metrics-data-transformer-function"

  val metrics: JList[String] = config.getStringList("data.metrics")
}
