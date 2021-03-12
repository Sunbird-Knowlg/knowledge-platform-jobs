package org.sunbird.job.task

import java.io.File
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.functions.EnrolmentReconciliationFn
import org.sunbird.job.util.FlinkUtil


class EnrolmentReconciliationStreamTask(config: EnrolmentReconciliationConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    val source = kafkaConnector.kafkaMapSource(config.kafkaInputTopic)

    env.addSource(source).name(config.enrolmentReconciliationConsumer)
      .uid(config.enrolmentReconciliationConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new EnrolmentReconciliationFn(config))
      .name("enrolment-reconciliation").uid("enrolment-reconciliation")
      .setParallelism(config.parallelism)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object RelationCacheUpdaterStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("enrolment-reconciliation.conf").withFallback(ConfigFactory.systemEnvironment()))
    val enrolmentReconciliationConfig = new EnrolmentReconciliationConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(enrolmentReconciliationConfig)
    val task = new EnrolmentReconciliationStreamTask(enrolmentReconciliationConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$
