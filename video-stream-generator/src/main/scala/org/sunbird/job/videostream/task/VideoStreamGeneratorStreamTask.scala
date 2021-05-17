package org.sunbird.job.videostream.task

import java.io.File
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.videostream.domain.Event
import org.sunbird.job.videostream.functions.VideoStreamGenerator
import org.sunbird.job.util.{FlinkUtil, HttpUtil}


class VideoStreamGeneratorStreamTask(config: VideoStreamGeneratorConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
    env.addSource(source).name(config.videoStreamConsumer)
      .uid(config.videoStreamConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .keyBy(_.identifier())
      .process(new VideoStreamGenerator(config, httpUtil))
      .name(config.videoStreamGeneratorFunction)
      .uid(config.videoStreamGeneratorFunction)
      .setParallelism(config.parallelism)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object VideoStreamGeneratorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("video-stream-generator.conf").withFallback(ConfigFactory.systemEnvironment()))
    val videoStreamConfig = new VideoStreamGeneratorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(videoStreamConfig)
    val httpUtil = new HttpUtil
    val task = new VideoStreamGeneratorStreamTask(videoStreamConfig, kafkaUtil, httpUtil)
    task.process()
  }
}

// $COVERAGE-ON$
