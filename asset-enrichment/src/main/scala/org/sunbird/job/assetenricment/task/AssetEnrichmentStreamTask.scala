package org.sunbird.job.assetenricment.task

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.domain.Event
import org.sunbird.job.functions.{AssetEnrichmentEventRouter, ImageEnrichmentFunction, VideoEnrichmentFunction}
import org.sunbird.job.util.FlinkUtil

class AssetEnrichmentStreamTask(config: AssetEnrichmentConfig, kafkaConnector: FlinkKafkaConnector) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
    val processStreamTask = env.addSource(source).name(config.assetEnrichmentConsumer)
      .uid(config.assetEnrichmentConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new AssetEnrichmentEventRouter(config))
      .name("asset-enrichment-router").uid("asset-enrichment-router")
      .setParallelism(config.eventRouterParallelism)

    processStreamTask.getSideOutput(config.imageEnrichmentDataOutTag).process(new ImageEnrichmentFunction(config))
      .name("image-enrichment-process").uid("image-enrichment-process").setParallelism(config.imageEnrichmentIndexerParallelism)

    val videoStream = processStreamTask.getSideOutput(config.videoEnrichmentDataOutTag).process(new VideoEnrichmentFunction(config))
      .name("video-enrichment-process").uid("video-enrichment-process").setParallelism(config.videoEnrichmentIndexerParallelism)

    videoStream.getSideOutput(config.generateVideoStreamingOutTag).addSink(kafkaConnector.kafkaStringSink(config.videoStreamingTopic))
    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object AssetEnrichmentStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("asset-enrichment.conf").withFallback(ConfigFactory.systemEnvironment()))
    val assetEnrichmentConfig = new AssetEnrichmentConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(assetEnrichmentConfig)
    val task = new AssetEnrichmentStreamTask(assetEnrichmentConfig, kafkaUtil)
    task.process()
  }

}
// $COVERAGE-ON$