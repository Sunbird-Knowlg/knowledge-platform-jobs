package org.sunbird.job.knowlg.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.knowlg.function.{CollectionPublishFunction, ContentPublishFunction, EnrichOnlyFunction, PublishEventRouter, QuestionPublishFunction, QuestionSetPublishFunction}
import org.sunbird.job.knowlg.publish.domain.Event
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File
import java.util

class KnowlgPublishStreamTask(config: KnowlgPublishConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  private implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
  private implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  private implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)

    val inputStream = env.fromSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic),
        WatermarkStrategy.noWatermarks(), config.inputConsumerName)
      .uid(config.inputConsumerName).setParallelism(config.kafkaConsumerParallelism)
      .rebalance

    buildGraph(env, inputStream)
    env.execute(config.jobName)
  }

  /** Test-facing entry point: supply a pre-built input stream. */
  def processForTest(env: StreamExecutionEnvironment, inputStream: DataStream[Event]): Unit = {
    buildGraph(env, inputStream)
    env.execute(config.jobName)
  }

  private def buildGraph(env: StreamExecutionEnvironment, inputStream: DataStream[Event]): Unit = {
    val processStreamTask = inputStream
      .process(new PublishEventRouter(config))
      .name("publish-event-router").uid("publish-event-router")
      .setParallelism(config.eventRouterParallelism)

    val contentPublish = processStreamTask.getSideOutput(config.contentPublishOutTag).process(new ContentPublishFunction(config, httpUtil))
      .name("content-publish-process").uid("content-publish-process").setParallelism(1)

    contentPublish.getSideOutput(config.generateVideoStreamingOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.postPublishTopic))
      .name("content-publish-postpublish-sink").uid("content-publish-postpublish-sink")
    contentPublish.getSideOutput(config.mvcProcessorTag).sinkTo(kafkaConnector.kafkaStringSink(config.mvcTopic))
      .name("content-publish-mvc-sink").uid("content-publish-mvc-sink")
    contentPublish.getSideOutput(config.contentMetadataEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.contentMetadataTopic))
      .name("content-publish-contentmetadata-sink").uid("content-publish-contentmetadata-sink")
    contentPublish.getSideOutput(config.enrichedMetadataEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.enrichedMetadataTopic))
      .name("content-publish-enrichedmetadata-sink").uid("content-publish-enrichedmetadata-sink")
    contentPublish.getSideOutput(config.failedEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))
      .name("content-publish-error-sink").uid("content-publish-error-sink")

    val collectionPublish = processStreamTask.getSideOutput(config.collectionPublishOutTag).process(new CollectionPublishFunction(config, httpUtil))
      .name("collection-publish-process").uid("collection-publish-process").setParallelism(1)
    collectionPublish.getSideOutput(config.generatePostPublishProcessTag).sinkTo(kafkaConnector.kafkaStringSink(config.postPublishTopic))
      .name("collection-publish-postpublish-sink").uid("collection-publish-postpublish-sink")
    collectionPublish.getSideOutput(config.enrichedMetadataEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.enrichedMetadataTopic))
      .name("collection-publish-enrichedmetadata-sink").uid("collection-publish-enrichedmetadata-sink")
    collectionPublish.getSideOutput(config.failedEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))
      .name("collection-publish-error-sink").uid("collection-publish-error-sink")

    if (config.enableDIALContextUpdate.equalsIgnoreCase("Yes")) {
      contentPublish.getSideOutput(config.dialcodeContextUpdaterOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.dialcodeContextUpdaterTopic))
        .name("content-publish-dialcode-sink").uid("content-publish-dialcode-sink")
      contentPublish.getSideOutput(config.qrimageOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.qrimageTopic))
        .name("content-publish-qrimage-sink").uid("content-publish-qrimage-sink")
      collectionPublish.getSideOutput(config.dialcodeContextUpdaterOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.dialcodeContextUpdaterTopic))
        .name("collection-publish-dialcode-sink").uid("collection-publish-dialcode-sink")
      collectionPublish.getSideOutput(config.qrimageOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.qrimageTopic))
        .name("collection-publish-qrimage-sink").uid("collection-publish-qrimage-sink")
    }

    val questionPublish = processStreamTask.getSideOutput(config.questionPublishOutTag).process(new QuestionPublishFunction(config, httpUtil))
      .name("question-publish-process").uid("question-publish-process").setParallelism(1)
    questionPublish.getSideOutput(config.enrichedMetadataEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.enrichedMetadataTopic))
      .name("question-publish-enrichedmetadata-sink").uid("question-publish-enrichedmetadata-sink")
    questionPublish.getSideOutput(config.failedEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))
      .name("question-publish-error-sink").uid("question-publish-error-sink")

    val questionSetPublish = processStreamTask.getSideOutput(config.questionSetPublishOutTag).process(new QuestionSetPublishFunction(config, httpUtil))
      .name("questionset-publish-process").uid("questionset-publish-process").setParallelism(1)
    questionSetPublish.getSideOutput(config.enrichedMetadataEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.enrichedMetadataTopic))
      .name("questionset-publish-enrichedmetadata-sink").uid("questionset-publish-enrichedmetadata-sink")
    questionSetPublish.getSideOutput(config.failedEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))
      .name("questionset-publish-error-sink").uid("questionset-publish-error-sink")

    val enrichOnly = processStreamTask.getSideOutput(config.enrichOnlyOutTag).process(new EnrichOnlyFunction(config))
      .name("enrich-only-process").uid("enrich-only-process").setParallelism(1)
    enrichOnly.getSideOutput(config.enrichedMetadataEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.enrichedMetadataTopic))
      .name("enrich-only-enrichedmetadata-sink").uid("enrich-only-enrichedmetadata-sink")
    enrichOnly.getSideOutput(config.failedEventOutTag).sinkTo(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))
      .name("enrich-only-error-sink").uid("enrich-only-error-sink")
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