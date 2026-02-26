package org.sunbird.job.transaction.task

import java.io.File
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.functions.{
  AuditEventGenerator,
  AuditHistoryIndexer,
  CompositeSearchIndexerFunction,
  DIALCodeIndexerFunction,
  DIALCodeMetricsIndexerFunction,
  ObsrvMetaDataGenerator,
  SearchIndexerRouter,
  TransactionEventRouter
}
import org.sunbird.job.util.{ElasticSearchUtil, FlinkUtil}

class TransactionEventProcessorStreamTask(
    config: TransactionEventProcessorConfig,
    kafkaConnector: FlinkKafkaConnector,
    esUtil: ElasticSearchUtil
) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment =
      FlinkUtil.getExecutionContext(config)
    //    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    implicit val eventTypeInfo: TypeInformation[Event] =
      TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] =
      TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] =
      TypeExtractor.getForClass(classOf[String])
    implicit val mapEsTypeInfo: TypeInformation[util.Map[String, Any]] =
      TypeExtractor.getForClass(classOf[util.Map[String, Any]])

    val source =
      kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
    val inputStream = env
      .addSource(source)
      .name(config.transactionEventConsumer)
      .uid(config.transactionEventConsumer)
      .setParallelism(config.kafkaConsumerParallelism)
      .rebalance

    val transactionSideOutput = inputStream
      .process(new TransactionEventRouter(config))
      .name(config.transactionEventRouterFunction)
      .uid(config.transactionEventRouterFunction)
      .setParallelism(config.parallelism)
      .getSideOutput(config.outputTag)

    if (config.auditEventGenerator) {
      val auditStream = transactionSideOutput
        .process(new AuditEventGenerator(config))
        .name(config.auditEventGeneratorFunction)
        .uid(config.auditEventGeneratorFunction)
        .setParallelism(config.parallelism)

      auditStream
        .getSideOutput(config.auditOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditOutputTopic))
        .name(config.auditEventProducer)
        .uid(config.auditEventProducer)
        .setParallelism(config.kafkaProducerParallelism)
    }

    if (config.obsrvMetadataGenerator) {
      val obsrvStream = transactionSideOutput
        .process(new ObsrvMetaDataGenerator(config))
        .name(config.obsrvMetaDataGeneratorFunction)
        .uid(config.obsrvMetaDataGeneratorFunction)
        .setParallelism(config.parallelism)

      obsrvStream
        .getSideOutput(config.obsrvAuditOutputTag)
        .addSink(kafkaConnector.kafkaStringSink(config.kafkaObsrvOutputTopic))
        .name(config.obsrvEventProducer)
        .uid(config.obsrvEventProducer)
        .setParallelism(config.kafkaProducerParallelism)
    }

    if (config.auditHistoryIndexer) {
      transactionSideOutput
        .keyBy(new TransactionEventKeySelector())
        .process(new AuditHistoryIndexer(config, esUtil))
        .name(config.auditHistoryIndexerFunction)
        .uid(config.auditHistoryIndexerFunction)
        .setParallelism(config.parallelism)
    }

    val searchIndexerStream = inputStream
      .keyBy(new TransactionEventKeySelector())
      .process(new SearchIndexerRouter(config))
      .name(config.transactionEventRouter)
      .uid(config.transactionEventRouter)
      .setParallelism(config.eventRouterParallelism)

    if (config.compositeSearchIndexer) {
      val compositeSearchStream = searchIndexerStream
        .getSideOutput(config.compositeSearchDataOutTag)
        .process(new CompositeSearchIndexerFunction(config))
        .name("composite-search-indexer")
        .uid("composite-search-indexer")
        .setParallelism(config.compositeSearchIndexerParallelism)

      compositeSearchStream
        .getSideOutput(config.failedEventOutTag)
        .addSink(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))
    }

    if (config.dialCodeIndexer) {
      val dialcodeExternalStream = searchIndexerStream
        .getSideOutput(config.dialCodeExternalOutTag)
        .process(new DIALCodeIndexerFunction(config))
        .name("dialcode-external-indexer")
        .uid("dialcode-external-indexer")
        .setParallelism(config.dialCodeExternalIndexerParallelism)

      dialcodeExternalStream
        .getSideOutput(config.failedEventOutTag)
        .addSink(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))
    }

    if (config.dialCodeMetricsIndexer) {
      val dialcodeMetricStream = searchIndexerStream
        .getSideOutput(config.dialCodeMetricOutTag)
        .process(new DIALCodeMetricsIndexerFunction(config))
        .name("dialcode-metric-indexer")
        .uid("dialcode-metric-indexer")
        .setParallelism(config.dialCodeMetricIndexerParallelism)

      dialcodeMetricStream
        .getSideOutput(config.failedEventOutTag)
        .addSink(kafkaConnector.kafkaStringSink(config.kafkaErrorTopic))
    }

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object TransactionEventProcessorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(
      ParameterTool.fromArgs(args).get("config.file.path")
    )
    val config = configFilePath
      .map { path =>
        ConfigFactory.parseFile(new File(path)).resolve()
      }
      .getOrElse(
        ConfigFactory
          .load("transaction-event-processor.conf")
          .withFallback(ConfigFactory.systemEnvironment())
      )
    val transactionEventProcessorConfig = new TransactionEventProcessorConfig(
      config
    )
    val kafkaUtil = new FlinkKafkaConnector(transactionEventProcessorConfig)
    val esUtil: ElasticSearchUtil = null
    val task = new TransactionEventProcessorStreamTask(
      transactionEventProcessorConfig,
      kafkaUtil,
      esUtil
    )
    task.process()
  }
}

// $COVERAGE-ON$

class TransactionEventKeySelector extends KeySelector[Event, String] {
  override def getKey(in: Event): String = in.id.replace(".img", "")
}
