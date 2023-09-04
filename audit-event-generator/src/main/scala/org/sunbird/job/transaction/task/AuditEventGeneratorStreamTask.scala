package org.sunbird.job.transaction.task

import java.io.File
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.functions.AuditEventGenerator
import org.sunbird.job.util.{FlinkUtil, HttpUtil}


class AuditEventGeneratorStreamTask(config: AuditEventGeneratorConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
//    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val processStreamTask = env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)).name(config.auditEventConsumer)
      .uid(config.auditEventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new AuditEventGenerator(config))
      .name(config.auditEventGeneratorFunction)
      .uid(config.auditEventGeneratorFunction)
      .setParallelism(config.parallelism)

    processStreamTask.getSideOutput(config.auditOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaOutputTopic))
      .name(config.auditEventProducer).uid(config.auditEventProducer).setParallelism(config.kafkaProducerParallelism)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object AuditEventGeneratorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("audit-event-generator.conf").withFallback(ConfigFactory.systemEnvironment()))
    val auditEventConfig = new AuditEventGeneratorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(auditEventConfig)
    val httpUtil = new HttpUtil
    val task = new AuditEventGeneratorStreamTask(auditEventConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$

class AuditEventKeySelector extends KeySelector[Event, String] {
  override def getKey(in: Event): String = in.id.replace(".img", "")
}