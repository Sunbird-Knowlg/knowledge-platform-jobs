package org.sunbird.job.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.functions.CertificatePreProcessor
import org.sunbird.job.util.FlinkUtil

class CertificatePreProcessorStreamTask(config: CertificatePreProcessorConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    val source = kafkaConnector.kafkaMapSource(config.kafkaInputTopic)

    val progressStream =
      env.addSource(source, config.certificatePreProcessorConsumer)
        .uid(config.certificatePreProcessorConsumer).setParallelism(config.kafkaConsumerParallelism)
        .rebalance()
        .process(new CertificatePreProcessor(config))
        .name("certificate-pre-processor").uid("certificate-pre-processor")
        .setParallelism(config.parallelism)

    progressStream.getSideOutput(config.generateCertificateOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaOutputTopic)).name(config.generateCertificateProducer).uid(config.generateCertificateProducer)
    progressStream.getSideOutput(config.failedEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic)).name(config.generateCertificateFailedEventProducer).uid(config.generateCertificateFailedEventProducer)
    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object CertificatePreProcessorStreamTask {
  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("certificate-pre-processor.conf").withFallback(ConfigFactory.systemEnvironment()))
    val certificatePreProcessorConfig = new CertificatePreProcessorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(certificatePreProcessorConfig)
    val task = new CertificatePreProcessorStreamTask(certificatePreProcessorConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$
