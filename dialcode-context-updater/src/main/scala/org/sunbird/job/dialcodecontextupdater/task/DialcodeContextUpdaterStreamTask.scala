package org.sunbird.job.dialcodecontextupdater.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.dialcodecontextupdater.domain.Event
import org.sunbird.job.dialcodecontextupdater.functions.DialcodeContextUpdaterFunction
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File
import java.util


class DialcodeContextUpdaterStreamTask(config: DialcodeContextUpdaterConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val processStreamTask = env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)).name(config.eventConsumer)
      .uid(config.eventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new DialcodeContextUpdaterFunction(config, httpUtil))
      .name(config.dialcodeContextUpdaterFunction)
      .uid(config.dialcodeContextUpdaterFunction)
      .setParallelism(config.parallelism)

    processStreamTask.getSideOutput(config.failedEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedTopic))

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object DialcodeContextUpdaterStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("dialcode-context-updater.conf").withFallback(ConfigFactory.systemEnvironment()))
    val contentAutoCreatorConfig = new DialcodeContextUpdaterConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(contentAutoCreatorConfig)
    val httpUtil = new HttpUtil
    val task = new DialcodeContextUpdaterStreamTask(contentAutoCreatorConfig, kafkaUtil, httpUtil)
    task.process()
  }
}

// $COVERAGE-ON$
