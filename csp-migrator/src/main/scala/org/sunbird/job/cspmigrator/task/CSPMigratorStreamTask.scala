package org.sunbird.job.cspmigrator.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.cspmigrator.domain.Event
import org.sunbird.job.cspmigrator.functions.{CSPNeo4jMigratorFunction, CSPCassandraMigratorFunction}
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File
import java.util


class CSPMigratorStreamTask(config: CSPMigratorConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val processStreamTask = env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)).name(config.eventConsumer)
      .uid(config.eventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new CSPNeo4jMigratorFunction(config, httpUtil))
      .name(config.cspMigratorFunction)
      .uid(config.cspMigratorFunction)
      .setParallelism(config.parallelism)


    processStreamTask.getSideOutput(config.cassandraMigrationOutputTag).process(new CSPCassandraMigratorFunction(config, httpUtil))
      .name(config.cassandraMigratorFunction).uid(config.cassandraMigratorFunction).setParallelism(config.cassandraMigratorParallelism)

    processStreamTask.getSideOutput(config.failedEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedTopic))
    processStreamTask.getSideOutput(config.generateVideoStreamingOutTag).addSink(kafkaConnector.kafkaStringSink(config.liveVideoStreamingTopic))
    processStreamTask.getSideOutput(config.liveContentNodePublishEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.liveNodeRepublishTopic))

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object CSPMigratorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("csp-migrator.conf").withFallback(ConfigFactory.systemEnvironment()))
    val CSPMigratorConfig = new CSPMigratorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(CSPMigratorConfig)
    val httpUtil = new HttpUtil
    val task = new CSPMigratorStreamTask(CSPMigratorConfig, kafkaUtil, httpUtil)
    task.process()
  }
}

// $COVERAGE-ON$
