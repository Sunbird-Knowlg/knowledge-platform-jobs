package org.sunbird.job.task

import java.io.File
import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.util.{FlinkUtil, HttpUtil}
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.sunbird.job.compositesearch.domain.Event
import org.sunbird.job.functions.{CompositeSearchEventRouter, CompositeSearchIndexerFunction, DialCodeExternalIndexerFunction, DialCodeMetricIndexerFunction}

class CompositeSearchIndexerStreamTask(config: CompositeSearchIndexerConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  def process(): Unit = {
    // TODO
    // Uncomment the below line and remove the line below it
    //    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)

    val processStreamTask = env.addSource(source).name(config.compositeSearchIndexerConsumer)
      .uid(config.compositeSearchIndexerConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new CompositeSearchEventRouter(config))
      .name("composite-search-router").uid("composite-search-router")
      .setParallelism(config.eventRouterParallelism)

    val compositeSearchStream = processStreamTask.getSideOutput(config.compositveSearchDataOutTag).process(new CompositeSearchIndexerFunction(config))
      .name("composite-search-indexer-process").uid("composite-search-indexer-process").setParallelism(config.compositeSearchIndexerParallelism)
    val dialcodeExternalStream = processStreamTask.getSideOutput(config.dialCodeExternalOutTag).process(new DialCodeExternalIndexerFunction(config))
      .name("dialcode-external-indexer-process").uid("dialcode-external-indexer-process").setParallelism(config.dialCodeExternalIndexerParallelism)
    val dialcodeMetricStream = processStreamTask.getSideOutput(config.dialCodeMetricOutTag).process(new DialCodeMetricIndexerFunction(config))
      .name("dialcode-metric-indexer-process").uid("dialcode-metric-indexer-process").setParallelism(config.dialCodeMetricInfexerParallelism)

    compositeSearchStream.getSideOutput(config.failedEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))
    dialcodeExternalStream.getSideOutput(config.failedEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))
    dialcodeMetricStream.getSideOutput(config.failedEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object CompositeSearchIndexerStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("composite-search-indexer.conf").withFallback(ConfigFactory.systemEnvironment()))
    val searchIndexerConfig = new CompositeSearchIndexerConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(searchIndexerConfig)
    val httpUtil = new HttpUtil()
    val task = new CompositeSearchIndexerStreamTask(searchIndexerConfig, kafkaUtil, httpUtil)
    task.process()
  }

}

// $COVERAGE-ON$
