package org.sunbird.job.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.BaseJobConfig


class CassandraDataMigrationConfig(override val config: Config) extends BaseJobConfig(config, "cassandra-data-migraton") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")
  val linkCollectionParallelism: Int = if (config.hasPath("task.link-collection.parallelism"))
    config.getInt("task.link-collection.parallelism") else 1

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-events-count"

  // Consumers
  val eventConsumer = "cassandra-data-migration-consumer"
  val cassandraDataMigrationFunction = "cassandra-data-migration-process"
  val cassandraDataMigrationEventProducer = "cassandra-data-migration-producer"

  val configVersion = "1.0"

  // DB Config
  val cassandraHost: String = config.getString("lms-cassandra.host")
  val cassandraPort: Int = config.getInt("lms-cassandra.port")

  val cassandraKeyspace: String = config.getString("migrate.keyspace")
  val cassandraTable: String = config.getString("migrate.table")
  val primaryKeyColumn: String = config.getString("migrate.primary_key_column")
  val columnToMigrate: String = config.getString("migrate.column_to_migrate")
  val keyStringToMigrate : String = config.getString("migrate.key_string_to_migrate")
  val valueStringToMigrate: String = config.getString("migrate.value_string_to_migrate")

  def getConfig() = config
}
