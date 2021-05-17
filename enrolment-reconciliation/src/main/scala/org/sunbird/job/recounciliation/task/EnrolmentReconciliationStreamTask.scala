package org.sunbird.job.recounciliation.task

import java.io.File
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.domain.CollectionProgress
import org.sunbird.job.functions.{EnrolmentReconciliationFn, ProgressCompleteFunction, ProgressUpdateFunction}
import org.sunbird.job.recounciliation.domain.CollectionProgress
import org.sunbird.job.util.{FlinkUtil, HttpUtil}
import org.sunbird.recounciliation.domain.CollectionProgress


class EnrolmentReconciliationStreamTask(config: EnrolmentReconciliationConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    implicit val enrolmentCompleteTypeInfo: TypeInformation[List[CollectionProgress]] = TypeExtractor.getForClass(classOf[List[CollectionProgress]])
    val source = kafkaConnector.kafkaMapSource(config.kafkaInputTopic)

    val progressStream = env.addSource(source).name(config.enrolmentReconciliationConsumer)
      .uid(config.enrolmentReconciliationConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new EnrolmentReconciliationFn(config, httpUtil))
      .name("enrolment-reconciliation").uid("enrolment-reconciliation")
      .setParallelism(config.enrolmentReconciliationParallelism)

    progressStream.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name(config.enrolmentReconciliationProducer).uid(config.enrolmentReconciliationProducer)
    progressStream.getSideOutput(config.failedEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic))
      .name(config.enrolmentReconciliationFailedEventProducer).uid(config.enrolmentReconciliationFailedEventProducer)

    // TODO: set separate parallelism for below task.
    progressStream.getSideOutput(config.collectionUpdateOutputTag).process(new ProgressUpdateFunction(config))
      .name(config.collectionProgressUpdateFn).uid(config.collectionProgressUpdateFn).setParallelism(config.enrolmentCompleteParallelism)
    val enrolmentCompleteStream = progressStream.getSideOutput(config.collectionCompleteOutputTag).process(new ProgressCompleteFunction(config))
      .name(config.collectionCompleteFn).uid(config.collectionCompleteFn).setParallelism(config.enrolmentCompleteParallelism)

    enrolmentCompleteStream.getSideOutput(config.certIssueOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaCertIssueTopic))
      .name(config.certIssueEventProducer).uid(config.certIssueEventProducer)
    enrolmentCompleteStream.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name(config.enrolmentCompleteEventProducer).uid(config.enrolmentCompleteEventProducer)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object EnrolmentReconciliationStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("enrolment-reconciliation.conf").withFallback(ConfigFactory.systemEnvironment()))
    val enrolmentReconciliationConfig = new EnrolmentReconciliationConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(enrolmentReconciliationConfig)
    val task = new EnrolmentReconciliationStreamTask(enrolmentReconciliationConfig, kafkaUtil, new HttpUtil)
    task.process()
  }
}

// $COVERAGE-ON$
