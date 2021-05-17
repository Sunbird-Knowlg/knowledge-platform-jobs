package org.sunbird.job.postpublish.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.postpublish.domain.Event
import org.sunbird.job.postpublish.functions._
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File
import java.util

class PostPublishProcessorStreamTask(config: PostPublishProcessorConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    implicit val publishMetaTypeInfo: TypeInformation[PublishMetadata] = TypeExtractor.getForClass(classOf[PublishMetadata])

    val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)

    val processStreamTask = env.addSource(source).name(config.inputConsumerName)
      .uid(config.inputConsumerName).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new PostPublishEventRouter(config, httpUtil))
      .name("post-publish-event-router").uid("post-publish-event-router")
      .setParallelism(config.eventRouterParallelism)

    processStreamTask.getSideOutput(config.batchCreateOutTag).process(new BatchCreateFunction(config, httpUtil))
      .name("batch-create-process").uid("batch-create-process").setParallelism(config.batchCreateParallelism)

    val shallowCopyPublishStream = processStreamTask.getSideOutput(config.shallowContentPublishOutTag)
      .process(new ShallowCopyPublishFunction(config))
      .name("shallow-content-publish").uid("shallow-content-publish")
      .setParallelism(config.shallowCopyParallelism)

    shallowCopyPublishStream.getSideOutput(config.publishEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.contentPublishTopic))
      .name("shallow-content-publish-producer").uid("shallow-content-publish-producer")

    val linkDialCodeStream = processStreamTask.getSideOutput(config.linkDIALCodeOutTag)
      .process(new DIALCodeLinkFunction(config, httpUtil))
      .name("dialcode-link-process").uid("dialcode-link-process").setParallelism(config.linkDialCodeParallelism)

    linkDialCodeStream.getSideOutput(config.generateQRImageOutTag).addSink(kafkaConnector.kafkaStringSink(config.QRImageGeneratorTopic))
    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object PostPublishProcessorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("post-publish-processor.conf").withFallback(ConfigFactory.systemEnvironment()))
    val pppConfig = new PostPublishProcessorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(pppConfig)
    val httpUtil = new HttpUtil
    val task = new PostPublishProcessorStreamTask(pppConfig, kafkaUtil, httpUtil)
    task.process()
  }

}

// $COVERAGE-ON$