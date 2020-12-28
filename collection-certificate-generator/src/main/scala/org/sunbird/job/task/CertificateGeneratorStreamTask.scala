package org.sunbird.job.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.functions.CertificateGeneratorFunction
import org.sunbird.job.util.{FlinkUtil, HttpUtil}
import org.sunbird.notifier.NotifierFunction
import org.sunbird.user.feeds.CreateUserFeedFunction

class CertificateGeneratorStreamTask(config: CertificateGeneratorConfig, kafkaConnector: FlinkKafkaConnector) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor
      .getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    val source = kafkaConnector.kafkaMapSource(config.kafkaInputTopic)


    val processStreamTask = env.addSource(source).name(config.certificateGeneratorConsumer)
      .uid(config.certificateGeneratorConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new CertificateGeneratorFunction(config))
      .name("collection-certificate-generator").uid("collection-certificate-generator")
      .setParallelism(config.parallelism)

    processStreamTask.getSideOutput(config.failedEventOutputTag)
      .addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic))
      .name(config.certificateGeneratorFailedEventProducer)
      .uid(config.certificateGeneratorFailedEventProducer)

    processStreamTask.getSideOutput(config.notifierOutputTag).process(new NotifierFunction(config)).name("notifier")
      .uid("notifier").setParallelism(1)
    processStreamTask.getSideOutput(config.userFeedOutputTag).process(new CreateUserFeedFunction(config))
      .name("user-feed").uid("user-feed").setParallelism(1)


    env.execute(config.jobName)
  }

}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object CertificateGeneratorStreamTask {
  var httpUtil = new HttpUtil

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("collection-certificate-generator.conf").withFallback(ConfigFactory.systemEnvironment()))
    val ccgConfig = new CertificateGeneratorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(ccgConfig)
    val task = new CertificateGeneratorStreamTask(ccgConfig, kafkaUtil)
    task.process()
  }
}
// $COVERAGE-ON$
