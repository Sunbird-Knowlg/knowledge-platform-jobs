package org.sunbird.job.contentautocreator.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.contentautocreator.domain.Event
import org.sunbird.job.contentautocreator.functions.ContentAutoCreatorFunction
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File
import java.util


class ContentAutoCreatorStreamTask(config: ContentAutoCreatorConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val processStreamTask = env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)).name(config.eventConsumer)
      .uid(config.eventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new ContentAutoCreatorFunction(config, httpUtil))
      .name(config.contentAutoCreatorFunction)
      .uid(config.contentAutoCreatorFunction)
      .setParallelism(config.parallelism)

    processStreamTask.getSideOutput(config.failedEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedTopic))

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object ContentAutoCreatorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("content-auto-creator.conf").withFallback(ConfigFactory.systemEnvironment()))
    val contentAutoCreatorConfig = new ContentAutoCreatorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(contentAutoCreatorConfig)
    val httpUtil = new HttpUtil
    val task = new ContentAutoCreatorStreamTask(contentAutoCreatorConfig, kafkaUtil, httpUtil)
    task.process()
  }
}

// $COVERAGE-ON$
