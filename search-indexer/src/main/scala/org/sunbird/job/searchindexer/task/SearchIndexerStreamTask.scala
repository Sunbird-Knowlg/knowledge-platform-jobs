package org.sunbird.job.searchindexer.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.searchindexer.compositesearch.domain.Event
import org.sunbird.job.searchindexer.functions.{CompositeSearchIndexerFunction, DIALCodeIndexerFunction, DIALCodeMetricsIndexerFunction, TransactionEventRouter}
import org.sunbird.job.util.FlinkUtil

import java.io.File
import java.util

class SearchIndexerStreamTask(config: SearchIndexerConfig, kafkaConnector: FlinkKafkaConnector) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
    val processStreamTask = env.addSource(source).name(config.searchIndexerConsumer)
      .uid(config.searchIndexerConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .keyBy(new SearchIndexerKeySelector())
      .process(new TransactionEventRouter(config))
      .name(config.transactionEventRouter).uid(config.transactionEventRouter)
      .setParallelism(config.eventRouterParallelism)

    val compositeSearchStream = processStreamTask.getSideOutput(config.compositeSearchDataOutTag).process(new CompositeSearchIndexerFunction(config))
      .name("composite-search-indexer").uid("composite-search-indexer").setParallelism(config.compositeSearchIndexerParallelism)
    val dialcodeExternalStream = processStreamTask.getSideOutput(config.dialCodeExternalOutTag).process(new DIALCodeIndexerFunction(config))
      .name("dialcode-external-indexer").uid("dialcode-external-indexer").setParallelism(config.dialCodeExternalIndexerParallelism)
    val dialcodeMetricStream = processStreamTask.getSideOutput(config.dialCodeMetricOutTag).process(new DIALCodeMetricsIndexerFunction(config))
      .name("dialcode-metric-indexer").uid("dialcode-metric-indexer").setParallelism(config.dialCodeMetricIndexerParallelism)

    compositeSearchStream.getSideOutput(config.failedEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))
    dialcodeExternalStream.getSideOutput(config.failedEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))
    dialcodeMetricStream.getSideOutput(config.failedEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object SearchIndexerStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("search-indexer.conf").withFallback(ConfigFactory.systemEnvironment()))
    val searchIndexerConfig = new SearchIndexerConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(searchIndexerConfig)
    val task = new SearchIndexerStreamTask(searchIndexerConfig, kafkaUtil)
    task.process()
  }

}

// $COVERAGE-ON$

class SearchIndexerKeySelector extends KeySelector[Event, String] {
  override def getKey(in: Event): String = in.id.replace(".img", "")
}