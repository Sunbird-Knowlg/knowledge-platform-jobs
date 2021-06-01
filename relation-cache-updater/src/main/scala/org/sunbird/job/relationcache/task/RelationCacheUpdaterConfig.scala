package org.sunbird.job.relationcache.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.BaseJobConfig

class RelationCacheUpdaterConfig(override val config: Config) extends BaseJobConfig(config, "relation-cache-updater") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"
  val cacheWrite = "cache-write-count"
  val dbReadCount = "db-read-count"

  // Consumers
  val relationCacheConsumer = "relation-cache-updater-consumer"

  // Cassandra Configurations
  val dbTable: String = config.getString("lms-cassandra.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val hierarchyPrimaryKey: List[String] = List("identifier")

  // Redis Configurations
  val relationCacheStore: Int = config.getInt("redis.database.index")
  val dpRedisHost: String = config.getString("dp-redis.host")
  val dpRedisPort: Int = config.getInt("dp-redis.port")
  val collectionCacheStore: Int = config.getInt("dp-redis.database.index")

}
