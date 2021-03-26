package org.sunbird.job.task

import java.io.File
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.audithistory.domain.Event
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.functions.AuditHistoryIndexer
import org.sunbird.job.util.{FlinkUtil, HttpUtil}


class AuditHistoryIndexerStreamTask(config: AuditHistoryIndexerConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)).name(config.eventConsumer)
      .uid(config.eventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new AuditHistoryIndexer(config))
      .name(config.auditHistoryIndexerFunction)
      .uid(config.auditHistoryIndexerFunction)
      .setParallelism(config.parallelism)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object AuditHistoryIndexerStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("audit-history-indexer.conf").withFallback(ConfigFactory.systemEnvironment()))
    val auditHistoryIndexerConfig = new AuditHistoryIndexerConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(auditHistoryIndexerConfig)
    val httpUtil = new HttpUtil
    val task = new AuditHistoryIndexerStreamTask(auditHistoryIndexerConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$
