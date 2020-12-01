package org.sunbird.job.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.domain.CollectionProgress
import org.sunbird.job.functions.{ActivityAggregatesFunction, ContentConsumptionDeDupFunction, CollectionProgressCompleteFunction}
import org.sunbird.job.util.FlinkUtil


class ActivityAggregateUpdaterStreamTask(config: ActivityAggregateUpdaterConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    implicit val enrolmentCompleteTypeInfo: TypeInformation[List[CollectionProgress]] = TypeExtractor.getForClass(classOf[List[CollectionProgress]])

    val progressStream =
      env.addSource(kafkaConnector.kafkaMapSource(config.kafkaInputTopic)).name(config.activityAggregateUpdaterConsumer)
        .uid(config.activityAggregateUpdaterConsumer).setParallelism(config.kafkaConsumerParallelism)
        .rebalance
        .process(new ContentConsumptionDeDupFunction(config)).name(config.consumptionDeDupFn)
        .uid(config.consumptionDeDupFn).setParallelism(config.deDupProcessParallelism)
        .getSideOutput(config.uniqueConsumptionOutput)
        .keyBy(new ActivityAggregatorKeySelector(config))
        .countWindow(config.thresholdBatchReadSize)
        .process(new ActivityAggregatesFunction(config))
        .name(config.activityAggregateUpdaterFn)
        .uid(config.activityAggregateUpdaterFn)
        .setParallelism(config.activityAggregateUpdaterParallelism)

    progressStream.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name(config.activityAggregateUpdaterProducer).uid(config.activityAggregateUpdaterProducer)
    progressStream.getSideOutput(config.failedEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic))
      .name(config.activityAggFailedEventProducer).uid(config.activityAggFailedEventProducer)

    val enrolmentCompleteStream = progressStream.getSideOutput(config.collectionCompleteOutputTag).process(new CollectionProgressCompleteFunction(config))
      .name(config.enrolmentCompleteFn).uid(config.enrolmentCompleteFn).setParallelism(config.enrolmentCompleteParallelism)

    enrolmentCompleteStream.getSideOutput(config.certIssueOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaCertIssueTopic))
      .name(config.certIssueEventProducer).uid(config.certIssueEventProducer)
    enrolmentCompleteStream.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name(config.enrolmentCompleteEventProducer).uid(config.enrolmentCompleteEventProducer)

    env.execute(config.jobName)
  }


}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object ActivityAggregateUpdaterStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("activity-aggregate-updater.conf").withFallback(ConfigFactory.systemEnvironment()))
    val courseAggregator = new ActivityAggregateUpdaterConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(courseAggregator)
    val task = new ActivityAggregateUpdaterStreamTask(courseAggregator, kafkaUtil)
    task.process()
  }
}
// $COVERAGE-ON$

class ActivityAggregatorKeySelector(config: ActivityAggregateUpdaterConfig) extends KeySelector[Map[String, AnyRef], Int] {
  private val serialVersionUID = 7267989625042068736L
  private val shards = config.windowShards
  override def getKey(in: Map[String, AnyRef]): Int = {
    in.getOrElse(config.userId, "").asInstanceOf[String].hashCode % shards
  }
}
