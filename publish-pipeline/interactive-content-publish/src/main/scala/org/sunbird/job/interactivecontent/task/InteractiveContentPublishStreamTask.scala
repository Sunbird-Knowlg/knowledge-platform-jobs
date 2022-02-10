package org.sunbird.job.interactivecontent.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.interactivecontent.function.{InteractiveContentFunction, PublishEventRouter}
import org.sunbird.job.interactivecontent.publish.domain.Event
import org.sunbird.job.util.HttpUtil

import java.io.File
import java.util

class InteractiveContentPublishStreamTask(config: InteractiveContentPublishConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
    val processStreamTask = env.addSource(source).name(config.inputConsumerName)
      .uid(config.inputConsumerName).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new PublishEventRouter(config))
      .name("publish-event-router").uid("publish-event-router")
      .setParallelism(config.eventRouterParallelism)

    val publishChain = processStreamTask.getSideOutput(config.publishChainEventOutTag).process(new InteractiveContentFunction(config))
      .name("interactive-content-publish-process").uid("interactive-content-publish-process").setParallelism(1)
    publishChain.getSideOutput(config.failedEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object InteractiveContentPublishStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("interactive-content-publish.conf").withFallback(ConfigFactory.systemEnvironment()))
    val publishConfig = new InteractiveContentPublishConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(publishConfig)
    val httpUtil = new HttpUtil
    val task = new InteractiveContentPublishStreamTask(publishConfig, kafkaUtil, httpUtil)
    task.process()
  }
}