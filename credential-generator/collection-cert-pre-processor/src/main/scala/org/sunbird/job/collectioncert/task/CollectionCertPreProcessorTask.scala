package org.sunbird.job.collectioncert.task

import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.collectioncert.domain.Event
import org.sunbird.job.collectioncert.functions.CollectionCertPreProcessorFn
import org.sunbird.job.connector.FlinkKafkaConnector
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
              .keyBy(new CollectionCertPreProcessorKeySelector())
              .process(new CollectionCertPreProcessorFn(config, httpUtil))
              .name("collection-cert-pre-processor").uid("collection-cert-pre-processor")
              .setParallelism(config.parallelism)

        progressStream.getSideOutput(config.generateCertificateOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaOutputTopic))
          .name(config.generateCertificateProducer).uid(config.generateCertificateProducer).setParallelism(config.generateCertificateParallelism)
        env.execute(config.jobName)
    }

}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster

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

// $COVERAGE-ON

class CollectionCertPreProcessorKeySelector extends KeySelector[Event, String] {
    override def getKey(event: Event): String = Set(event.userId, event.courseId, event.batchId).mkString("_")
}
