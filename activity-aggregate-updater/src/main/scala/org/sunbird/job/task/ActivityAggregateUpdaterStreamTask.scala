package org.sunbird.job.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.domain.EnrolmentComplete
import org.sunbird.job.functions.{ActivityAggregatesFunction, EnrolmentCompleteFunction}
import org.sunbird.job.trigger.CountTriggerWithTimeout
import org.sunbird.job.util.FlinkUtil


class ActivityAggregateUpdaterStreamTask(config: ActivityAggregateUpdaterConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    implicit val enrolmentCompleteTypeInfo: TypeInformation[List[EnrolmentComplete]] = TypeExtractor.getForClass(classOf[List[EnrolmentComplete]])
    val progressStream =
      env.addSource(kafkaConnector.kafkaMapSource(config.kafkaInputTopic), config.activityAggregateUpdaterConsumer)
        .uid(config.activityAggregateUpdaterConsumer).setParallelism(config.kafkaConsumerParallelism)
        .rebalance()
        // .keyBy(x => x.get("partition").toString)
        .keyBy(new ActivityAggregatorKeySelector(config))
        .timeWindow(Time.seconds(config.thresholdBatchReadInterval))
        .trigger(new CountTriggerWithTimeout[TimeWindow](config.thresholdBatchReadSize, env.getStreamTimeCharacteristic))
        .process(new ActivityAggregatesFunction(config))
        .name(config.activityAggregateUpdaterFn)
        .uid(config.activityAggregateUpdaterFn)
        .setParallelism(config.activityAggregateUpdaterParallelism)

    val enrolmentCompleteStream = progressStream.getSideOutput(config.enrolmentCompleteOutputTag).rebalance().process(new EnrolmentCompleteFunction(config))
      .name(config.enrolmentCompleteFn).uid(config.enrolmentCompleteFn).setParallelism(config.enrolmentCompleteParallelism)

    enrolmentCompleteStream.getSideOutput(config.certIssueOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaCertIssueTopic))
      .name(config.certIssueEventProducer).uid(config.certIssueEventProducer)

    progressStream.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name(config.activityAggregateUpdaterProducer).uid(config.activityAggregateUpdaterProducer)
    progressStream.getSideOutput(config.failedEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic))
      .name(config.activityAggFailedEventProducer).uid(config.activityAggFailedEventProducer)
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

class ActivityAggregatorKeySelector(config: ActivityAggregateUpdaterConfig) extends KeySelector[util.Map[String, AnyRef], Int] {

  private val serialVersionUID = - 7160298986929432742L

  val shards: Int = config.windowShards
  override def getKey(in: util.Map[String, AnyRef]): Int = {
    in.getOrDefault(config.userId, "").asInstanceOf[String].hashCode % shards
  }
}
