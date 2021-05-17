package org.sunbird.job.relationcache.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.functions.RelationCacheUpdater
import org.sunbird.job.util.FlinkUtil


class RelationCacheUpdaterStreamTask(config: RelationCacheUpdaterConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    val source = kafkaConnector.kafkaMapSource(config.kafkaInputTopic)

    env.addSource(source).name(config.relationCacheConsumer)
      .uid(config.relationCacheConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new RelationCacheUpdater(config))
      .name("relation-cache-updater").uid("relation-cache-updater")
      .setParallelism(config.parallelism)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object RelationCacheUpdaterStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("relation-cache-updater.conf").withFallback(ConfigFactory.systemEnvironment()))
    val cacheUpdaterConfig = new RelationCacheUpdaterConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(cacheUpdaterConfig)
    val task = new RelationCacheUpdaterStreamTask(cacheUpdaterConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$
