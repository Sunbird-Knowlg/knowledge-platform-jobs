package org.sunbird.kp.course.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.sunbird.async.core.job.FlinkKafkaConnector
import org.sunbird.async.core.util.FlinkUtil
import org.sunbird.kp.course.functions.ProgressUpdater


class CourseMetricsAggregatorStreamTask(config: CourseMetricsAggregatorConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    env.addSource(kafkaConnector.kafkaMapSource(config.kafkaInputTopic), config.aggregatorConsumer)
   val dataStream = env.addSource(kafkaConnector.kafkaMapSource(config.kafkaInputTopic), "course-metrics-agg-consumer")
      .uid(config.aggregatorConsumer).setParallelism(config.kafkaConsumerParallelism)
      .keyBy(x=>  x.get("partition").toString)
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .process(new ProgressUpdater(config))
    env.execute(config.jobName)
  }

}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object CourseMetricsAggregatorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("course-metrics-aggregator.conf").withFallback(ConfigFactory.systemEnvironment()))
    val courseAggregator = new CourseMetricsAggregatorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(courseAggregator)
    val task = new CourseMetricsAggregatorStreamTask(courseAggregator, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$
