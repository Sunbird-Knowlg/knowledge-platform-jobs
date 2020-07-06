package org.sunbird.kp.flink.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.async.core.BaseJobConfig

class RelationCacheUpdaterConfig(override val config: Config) extends BaseJobConfig(config, "relation-cache-updater") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val eventMaxSize: Long = config.getLong("kafka.event.max.size")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"

  // Tags
  val failedEventOutputTagName = "failed-events"
  val successEventOutputTagName = "success-events"

  val successEventOutputTag: OutputTag[String] = OutputTag[String](successEventOutputTagName)
  val failedEventsOutputTag: OutputTag[String] = OutputTag[String](failedEventOutputTagName)

  // Consumers
  val aggregatorConsumer = "relation-cache-updater-consumer"

  // Cassandra Configurations
  val dbTable: String = config.getString("lp-cassandra.table")
  val dbKeyspace: String = config.getString("lp-cassandra.keyspace")
  val dbHost: String = config.getString("lp-cassandra.host")
  val dbPort: Int = config.getInt("lp-cassandra.port")
  val hierarchyPrimaryKey: List[String] = List("identifier")

  // Redis Configurations
  val leafNodesStore: Int = config.getInt("redis.database.leafnodes.id")

}
