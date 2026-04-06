package org.sunbird.job.qrimagegenerator.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.qrimagegenerator.domain.Event
import org.sunbird.job.qrimagegenerator.functions.{QRCodeImageGeneratorFunction, QRCodeIndexImageUrlFunction}
import org.sunbird.job.util.FlinkUtil

import java.io.File


class QRCodeImageGeneratorTask(config: QRCodeImageGeneratorConfig, kafkaConnector: FlinkKafkaConnector) {

  private implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
  private implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)

    val inputStream = env.fromSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic),
        WatermarkStrategy.noWatermarks(), config.eventConsumer)
      .uid(config.eventConsumer)
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
    val streamTask = inputStream
      .process(new QRCodeImageGeneratorFunction(config))
      .setParallelism(config.kafkaConsumerParallelism)
      .name(config.qrCodeImageGeneratorFunction)
      .uid(config.qrCodeImageGeneratorFunction)
      .setParallelism(config.parallelism)

    if (config.indexImageURL)
      streamTask.getSideOutput(config.indexImageUrlOutTag).process(new QRCodeIndexImageUrlFunction(config))
        .name("index-imageUrl-process").uid("index-imageUrl-process").setParallelism(config.parallelism)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object QRCodeImageGeneratorTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("qrcode-image-generator.conf").withFallback(ConfigFactory.systemEnvironment()))
    val qrCodeImageGeneratorConfig = new QRCodeImageGeneratorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(qrCodeImageGeneratorConfig)
    val task = new QRCodeImageGeneratorTask(qrCodeImageGeneratorConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$
