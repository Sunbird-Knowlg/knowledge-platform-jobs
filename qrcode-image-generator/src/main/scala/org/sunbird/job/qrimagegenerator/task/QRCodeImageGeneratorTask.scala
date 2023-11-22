package org.sunbird.job.qrimagegenerator.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.qrimagegenerator.domain.Event
import org.sunbird.job.qrimagegenerator.functions.{QRCodeImageGeneratorFunction, QRCodeIndexImageUrlFunction}
import org.sunbird.job.util.FlinkUtil

import java.io.File


class QRCodeImageGeneratorTask(config: QRCodeImageGeneratorConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
    val streamTask = env.fromSource(source, WatermarkStrategy.noWatermarks[Event](),  config.eventConsumer)
      .uid(config.eventConsumer)
      .rebalance
      .process(new QRCodeImageGeneratorFunction(config))
      .setParallelism(config.kafkaConsumerParallelism)
      .name(config.qrCodeImageGeneratorFunction)
      .uid(config.qrCodeImageGeneratorFunction)
      .setParallelism(config.parallelism)

    if(config.indexImageURL)
      streamTask.getSideOutput(config.indexImageUrlOutTag).process(new QRCodeIndexImageUrlFunction(config))
        .name("index-imageUrl-process").uid("index-imageUrl-process").setParallelism(config.parallelism)

    env.execute(config.jobName)
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
