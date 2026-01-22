package org.sunbird.job.transaction.task

import java.io.File
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.transaction.functions.{AuditEventGenerator, AuditHistoryIndexer, ObsrvMetaDataGenerator, TransactionEventRouter}
import org.sunbird.job.util.{ElasticSearchUtil, FlinkUtil}


class TransactionEventProcessorStreamTask(config: TransactionEventProcessorConfig, kafkaConnector: FlinkKafkaConnector, esUtil: ElasticSearchUtil) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    //    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    implicit val mapEsTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])

    val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
    val inputStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), config.transactionEventConsumer)
      .uid(config.transactionEventConsumer).setParallelism(config.kafkaConsumerParallelism)

    val processorStreamTask = inputStream.rebalance
      .process(new TransactionEventRouter(config))
      .name(config.transactionEventRouterFunction)
      .uid(config.transactionEventRouterFunction)
      .setParallelism(config.parallelism)

    val sideOutput = processorStreamTask.getSideOutput(config.outputTag)

    if (config.auditEventGenerator) {
      val auditStream = sideOutput.process(new AuditEventGenerator(config)).name(config.auditEventGeneratorFunction)
        .uid(config.auditEventGeneratorFunction).setParallelism(config.parallelism)

      auditStream.getSideOutput(config.auditOutputTag).sinkTo(kafkaConnector.kafkaStringSink(config.kafkaAuditOutputTopic))
        .name(config.auditEventProducer).uid(config.auditEventProducer).setParallelism(config.kafkaProducerParallelism)
    }

    if (config.obsrvMetadataGenerator) {
      val obsrvStream = sideOutput.process(new ObsrvMetaDataGenerator(config)).name(config.obsrvMetaDataGeneratorFunction)
        .uid(config.obsrvMetaDataGeneratorFunction).setParallelism(config.parallelism)

      obsrvStream.getSideOutput(config.obsrvAuditOutputTag).sinkTo(kafkaConnector.kafkaStringSink(config.kafkaObsrvOutputTopic))
        .name(config.obsrvEventProducer).uid(config.obsrvEventProducer).setParallelism(config.kafkaProducerParallelism)
    }

    if (config.auditHistoryIndexer) {
      sideOutput.keyBy(new TransactionEventKeySelector).process(new AuditHistoryIndexer(config, esUtil)).name(config.auditHistoryIndexerFunction)
        .uid(config.auditHistoryIndexerFunction).setParallelism(config.parallelism)
    }

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object TransactionEventProcessorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("transaction-event-processor.conf").withFallback(ConfigFactory.systemEnvironment()))
    val transactionEventProcessorConfig = new TransactionEventProcessorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(transactionEventProcessorConfig)
    val esUtil: ElasticSearchUtil = null
    val task = new TransactionEventProcessorStreamTask(transactionEventProcessorConfig, kafkaUtil, esUtil)
    task.process()
  }
}

// $COVERAGE-ON$

class TransactionEventKeySelector extends KeySelector[Event, String] {
  override def getKey(in: Event): String = in.id.replace(".img", "")
}
