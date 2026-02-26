package org.sunbird.job.livenodepublisher.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.livenodepublisher.function.{LiveCollectionPublishFunction, LiveContentPublishFunction, LivePublishEventRouter}
import org.sunbird.job.livenodepublisher.publish.domain.Event
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File
import java.util

class LiveNodePublisherStreamTask(config: LiveNodePublisherConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
//    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val source = kafkaConnector.kafkaJobRequestSourceV2[Event](config.kafkaInputTopic)
    val processStreamTask = env.fromSource(source, WatermarkStrategy.noWatermarks(), config.inputConsumerName)
      .uid(config.inputConsumerName).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new LivePublishEventRouter(config))
      .name("publish-event-router").uid("publish-event-router")
      .setParallelism(config.eventRouterParallelism)

    val contentPublish = processStreamTask.getSideOutput(config.contentPublishOutTag).process(new LiveContentPublishFunction(config, httpUtil))
      .name("live-content-publish-process").uid("live-content-publish-process").setParallelism(config.kafkaConsumerParallelism)

    contentPublish.getSideOutput(config.generateVideoStreamingOutTag).sinkTo(kafkaConnector.kafkaStringSinkV2(config.liveVideoStreamTopic))
    contentPublish.getSideOutput(config.failedEventOutTag).sinkTo(kafkaConnector.kafkaStringSinkV2(config.kafkaErrorTopic))

   val collectionPublish = processStreamTask.getSideOutput(config.collectionPublishOutTag).process(new LiveCollectionPublishFunction(config, httpUtil))
    		  .name("live-collection-publish-process").uid("live-collection-publish-process").setParallelism(config.kafkaConsumerParallelism)
    collectionPublish.getSideOutput(config.skippedEventOutTag).sinkTo(kafkaConnector.kafkaStringSinkV2(config.kafkaSkippedTopic))
    collectionPublish.getSideOutput(config.failedEventOutTag).sinkTo(kafkaConnector.kafkaStringSinkV2(config.kafkaErrorTopic))

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object LiveNodePublisherStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("live-node-publisher.conf").withFallback(ConfigFactory.systemEnvironment()))
    val publishConfig = new LiveNodePublisherConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(publishConfig)
    val httpUtil = new HttpUtil
    val task = new LiveNodePublisherStreamTask(publishConfig, kafkaUtil, httpUtil)
    task.process()
  }
}