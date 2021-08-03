package org.sunbird.job.metricstransformer.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.metricstransformer.domain.Event
import org.sunbird.job.metricstransformer.function.MetricsDataTransformerFunction
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.util.{ElasticSearchUtil, FlinkUtil, HttpUtil}

class MetricsDataTransformerStreamTask(config: MetricsDataTransformerConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)).name(config.eventConsumer)
      .uid(config.eventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new MetricsDataTransformerFunction(config, httpUtil))
      .name(config.metricsDataTransformerFunction)
      .uid(config.metricsDataTransformerFunction)
      .setParallelism(config.parallelism)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object MetricsDataTransformerStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("metrics-data-transformer.conf").withFallback(ConfigFactory.systemEnvironment()))
    val metricsDataTransformerConfig = new MetricsDataTransformerConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(metricsDataTransformerConfig)
    val httpUtil = new HttpUtil()
    val task = new MetricsDataTransformerStreamTask(metricsDataTransformerConfig, kafkaUtil, httpUtil)
    task.process()
  }

}

// $COVERAGE-ON$