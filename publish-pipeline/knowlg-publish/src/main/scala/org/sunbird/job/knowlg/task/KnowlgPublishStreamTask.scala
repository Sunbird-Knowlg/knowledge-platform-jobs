package org.sunbird.job.knowlg.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.knowlg.function.{CollectionPublishFunction, ContentPublishFunction, PublishEventRouter, QuestionPublishFunction, QuestionSetPublishFunction}
import org.sunbird.job.knowlg.publish.domain.Event
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File
import java.util

class KnowlgPublishStreamTask(config: KnowlgPublishConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
    val processStreamTask = env.fromSource(source, WatermarkStrategy.noWatermarks(), config.inputConsumerName)
      .uid(config.inputConsumerName).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new PublishEventRouter(config))
      .name("publish-event-router").uid("publish-event-router")
      .setParallelism(config.eventRouterParallelism)

    val contentPublish = processStreamTask.getSideOutput(config.contentPublishOutTag).process(new ContentPublishFunction(config, httpUtil))
      .name("content-publish-process").uid("content-publish-process").setParallelism(1)

    contentPublish.getSideOutput(config.generateVideoStreamingOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.postPublishTopic))
    contentPublish.getSideOutput(config.mvcProcessorTag).sinkTo(kafkaConnector.kafkaStringSink(config.mvcTopic))
    contentPublish.getSideOutput(config.contentMetadataEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.contentMetadataTopic))
    contentPublish.getSideOutput(config.failedEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))

   val collectionPublish = processStreamTask.getSideOutput(config.collectionPublishOutTag).process(new CollectionPublishFunction(config, httpUtil))
    		  .name("collection-publish-process").uid("collection-publish-process").setParallelism(1)
    collectionPublish.getSideOutput(config.generatePostPublishProcessTag).sinkTo(kafkaConnector.kafkaStringSink(config.postPublishTopic))
    collectionPublish.getSideOutput(config.failedEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))

    val questionPublish = processStreamTask.getSideOutput(config.questionPublishOutTag).process(new QuestionPublishFunction(config, httpUtil))
      .name("question-publish-process").uid("question-publish-process").setParallelism(1)
    questionPublish.getSideOutput(config.failedEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))

    val questionSetPublish = processStreamTask.getSideOutput(config.questionSetPublishOutTag).process(new QuestionSetPublishFunction(config, httpUtil))
      .name("questionset-publish-process").uid("questionset-publish-process").setParallelism(1)
    questionSetPublish.getSideOutput(config.failedEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object KnowlgPublishStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("content-publish.conf").withFallback(ConfigFactory.systemEnvironment()))
    val publishConfig = new KnowlgPublishConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(publishConfig)
    val httpUtil = new HttpUtil
    val task = new KnowlgPublishStreamTask(publishConfig, kafkaUtil, httpUtil)
    task.process()
  }
}
