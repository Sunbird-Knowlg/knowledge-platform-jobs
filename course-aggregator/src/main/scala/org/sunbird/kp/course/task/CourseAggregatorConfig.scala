package org.sunbird.kp.course.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.async.core.BaseJobConfig

class CourseAggregatorConfig(override val config: Config) extends BaseJobConfig(config, "CourseAggregator") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaFailedTopic: String = config.getString("kafka.output.failed.topic")
  val eventMaxSize: Long = config.getLong("kafka.event.max.size")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val aggregatorParallelism: Int = config.getInt("task.aggregator.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val dbUpdateCount = "db-update-count"
  val dbReadCount = "db-read-count"
  val skippedEventCount = "skipped-event-count"

  // Cassandra Configurations
  val dbTable: String = config.getString("lms-cassandra.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")

  // Redis Configurations
  val leafNodesStore: Int = config.getInt("redis.database.leafnodes.id")

  // Tags
  val FAILED_EVENTS_OUTPUT_TAG = "failed-events"
  val SUCCESS_EVENTS_OUTPUT_TAG = "success-events"

  val successEventOutputTag: OutputTag[util.Map[String, AnyRef]] = OutputTag[util.Map[String, AnyRef]](SUCCESS_EVENTS_OUTPUT_TAG)
  val failedEventsOutputTag: OutputTag[util.Map[String, AnyRef]] = OutputTag[util.Map[String, AnyRef]](FAILED_EVENTS_OUTPUT_TAG)

  // Consumers
  val aggregatorConsumer = "course-aggregator-consumer"

  // Producers
  val aggregatorProducer = "extractor-duplicate-events-sink"

  val completedStatusCode: Int = 2
  val inCompleteStatusCode: Int = 1
  val completionPercentage: Int = 100


}
