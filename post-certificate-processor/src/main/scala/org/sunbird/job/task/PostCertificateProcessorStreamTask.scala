package org.sunbird.job.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.functions.PostCertificateProcessFunction
import org.sunbird.job.util.{FlinkUtil, HttpUtil}


class PostCertificateProcessorStreamTask(config: PostCertificateProcessorConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    val source = kafkaConnector.kafkaMapSource(config.kafkaInputTopic)
    val processStreamTask = env.addSource(source, config.postCertificateProcessConsumer)
      .uid(config.postCertificateProcessConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
      .process(new PostCertificateProcessFunction(config))
      .name("post-certificate-process").uid("post-certificate-process")
      .setParallelism(config.parallelism)

    processStreamTask.getSideOutput(config.failedEventOutputTag)
      .addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic))
      .name(config.postCertificateProcessFailedEventProducer)
      .uid(config.postCertificateProcessFailedEventProducer)

    processStreamTask.getSideOutput(config.auditEventOutputTag)
      .addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name(config.postCertificateProcessAuditProducer)
      .uid(config.postCertificateProcessAuditProducer)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object PostCertificateProcessorStreamTask {

  var httpUtil = new HttpUtil

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("post-certificate-processor.conf").withFallback(ConfigFactory.systemEnvironment()))
    val postCertificateProcessorConfig = new PostCertificateProcessorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(postCertificateProcessorConfig)
    val task = new PostCertificateProcessorStreamTask(postCertificateProcessorConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$
