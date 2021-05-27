package org.sunbird.job.cert.task

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.collectioncert.domain.Event
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.functions.CollectionCertPreProcessorFn
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

class CollectionCertPreProcessorTask(config: CollectionCertPreProcessorConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {
    def process(): Unit = {
        implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
        implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
        implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
        val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)

        val progressStream =
            env.addSource(source).name(config.certificatePreProcessorConsumer)
              .uid(config.certificatePreProcessorConsumer).setParallelism(config.kafkaConsumerParallelism)
              .rebalance
              .process(new CollectionCertPreProcessorFn(config, httpUtil))
              .name("collection-cert-pre-processor").uid("collection-cert-pre-processor")
              .setParallelism(config.parallelism)

        progressStream.getSideOutput(config.generateCertificateOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaOutputTopic))
          .name(config.generateCertificateProducer).uid(config.generateCertificateProducer).setParallelism(config.generateCertificateParallelism)
        progressStream.getSideOutput(config.failedEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic)).name(config.generateCertificateFailedEventProducer).uid(config.generateCertificateFailedEventProducer)
        env.execute(config.jobName)
    }

}

object CollectionCertPreProcessorTask {
    def main(args: Array[String]): Unit = {
        val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
        val config = configFilePath.map {
            path => ConfigFactory.parseFile(new File(path)).resolve()
        }.getOrElse(ConfigFactory.load("collection-cert-pre-processor.conf").withFallback(ConfigFactory.systemEnvironment()))
        val certificatePreProcessorConfig = new CollectionCertPreProcessorConfig(config)
        val kafkaUtil = new FlinkKafkaConnector(certificatePreProcessorConfig)
        val httpUtil = new HttpUtil()
        val task = new CollectionCertPreProcessorTask(certificatePreProcessorConfig, kafkaUtil, httpUtil)
        task.process()
    }
}
