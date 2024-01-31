package org.sunbird.job.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.migration.domain.Event
import org.sunbird.job.migration.functions.CassandraDataMigrationFunction
import org.sunbird.job.util.FlinkUtil

import java.io.File
import java.util


class CassandraDataMigrationStreamTask(config: CassandraDataMigrationConfig, kafkaConnector: FlinkKafkaConnector) {
  private[this] val logger = LoggerFactory.getLogger(classOf[CassandraDataMigrationStreamTask])

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val cassandraDataMigratorStream = env.fromSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic), WatermarkStrategy.noWatermarks[Event](), config.eventConsumer)
      .uid(config.eventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new CassandraDataMigrationFunction(config))
      .name(config.cassandraDataMigrationFunction)
      .uid(config.cassandraDataMigrationFunction)
      .setParallelism(config.parallelism)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object CassandraDataMigrationStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("cassandra-data-migration.conf").withFallback(ConfigFactory.systemEnvironment()))
    val cassandraDataMigratorConfig = new CassandraDataMigrationConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(cassandraDataMigratorConfig)
    val task = new CassandraDataMigrationStreamTask(cassandraDataMigratorConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$
