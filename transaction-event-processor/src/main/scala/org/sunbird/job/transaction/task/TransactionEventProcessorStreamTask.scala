package org.sunbird.job.transaction.task

import java.io.File
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
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

  private implicit val eventTypeInfo: TypeInformation[Event] =
    TypeExtractor.getForClass(classOf[Event])
  private implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] =
    TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  private implicit val stringTypeInfo: TypeInformation[String] =
    TypeExtractor.getForClass(classOf[String])
  private implicit val mapEsTypeInfo: TypeInformation[util.Map[String, Any]] =
    TypeExtractor.getForClass(classOf[util.Map[String, Any]])

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment =
      FlinkUtil.getExecutionContext(config)

    val source =
      kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
    val inputStream = env
      .fromSource(source, WatermarkStrategy.noWatermarks(), config.transactionEventConsumer)
      .uid(config.transactionEventConsumer)
      .setParallelism(config.kafkaConsumerParallelism)
      .rebalance

    buildGraph(env, inputStream)
    env.execute(config.jobName)
  }

  /** Test-facing entry point: supply a pre-built input stream. */
  def processForTest(
      env: StreamExecutionEnvironment,
      inputStream: DataStream[Event]
  ): Unit = {
    buildGraph(env, inputStream)
    env.execute(config.jobName)
  }

  /** Routes a string stream to the Kafka sink for the given topic.
    * Overridable in tests to redirect output to an in-memory sink.
    */
  protected def stringSink(
      stream: DataStream[String],
      topic: String,
      name: String,
      uid: String,
      parallelism: Int
  ): Unit = {
    stream
      .sinkTo(kafkaConnector.kafkaStringSink(topic))
      .name(name)
      .uid(uid)
      .setParallelism(parallelism)
  }

  private def buildGraph(
      env: StreamExecutionEnvironment,
      inputStream: DataStream[Event]
  ): Unit = {

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

      stringSink(
        auditStream.getSideOutput(config.auditOutputTag),
        config.kafkaAuditOutputTopic,
        config.auditEventProducer,
        config.auditEventProducer,
        config.kafkaProducerParallelism
      )
    }

    if (config.obsrvMetadataGenerator) {
      val obsrvStream = transactionSideOutput
        .process(new ObsrvMetaDataGenerator(config))
        .name(config.obsrvMetaDataGeneratorFunction)
        .uid(config.obsrvMetaDataGeneratorFunction)
        .setParallelism(config.parallelism)

      stringSink(
        obsrvStream.getSideOutput(config.obsrvAuditOutputTag),
        config.kafkaObsrvOutputTopic,
        config.obsrvEventProducer,
        config.obsrvEventProducer,
        config.kafkaProducerParallelism
      )
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

      stringSink(
        compositeSearchStream.getSideOutput(config.failedEventOutTag),
        config.kafkaErrorTopic,
        "composite-search-failed-producer",
        "composite-search-failed-producer",
        config.kafkaProducerParallelism
      )
    }

    if (config.dialCodeIndexer) {
      val dialcodeExternalStream = searchIndexerStream
        .getSideOutput(config.dialCodeExternalOutTag)
        .process(new DIALCodeIndexerFunction(config))
        .name("dialcode-external-indexer")
        .uid("dialcode-external-indexer")
        .setParallelism(config.dialCodeExternalIndexerParallelism)

      stringSink(
        dialcodeExternalStream.getSideOutput(config.failedEventOutTag),
        config.kafkaErrorTopic,
        "dialcode-external-failed-producer",
        "dialcode-external-failed-producer",
        config.kafkaProducerParallelism
      )
    }

    if (config.dialCodeMetricsIndexer) {
      val dialcodeMetricStream = searchIndexerStream
        .getSideOutput(config.dialCodeMetricOutTag)
        .process(new DIALCodeMetricsIndexerFunction(config))
        .name("dialcode-metric-indexer")
        .uid("dialcode-metric-indexer")
        .setParallelism(config.dialCodeMetricIndexerParallelism)

      stringSink(
        dialcodeMetricStream.getSideOutput(config.failedEventOutTag),
        config.kafkaErrorTopic,
        "dialcode-metric-failed-producer",
        "dialcode-metric-failed-producer",
        config.kafkaProducerParallelism
      )
    }
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
