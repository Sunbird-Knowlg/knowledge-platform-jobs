package org.sunbird.job.audithistory.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.audithistory.domain.Event
import org.sunbird.job.audithistory.functions.AuditHistoryIndexer
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.util.{ElasticSearchUtil, FlinkUtil}

import java.io.File
import java.util


class AuditHistoryIndexerStreamTask(config: AuditHistoryIndexerConfig, kafkaConnector: FlinkKafkaConnector, esUtil: ElasticSearchUtil) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
//    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)).name(config.eventConsumer)
      .uid(config.eventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .keyBy(new AuditHistoryIndexerKeySelector)
      .process(new AuditHistoryIndexer(config, esUtil))
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
    val esUtil:ElasticSearchUtil = null
    val task = new AuditHistoryIndexerStreamTask(auditHistoryIndexerConfig, kafkaUtil, esUtil)
    task.process()
  }
}

// $COVERAGE-ON$

class AuditHistoryIndexerKeySelector extends KeySelector[Event, String] {
  override def getKey(in: Event): String = in.id.replace(".img", "")
}
