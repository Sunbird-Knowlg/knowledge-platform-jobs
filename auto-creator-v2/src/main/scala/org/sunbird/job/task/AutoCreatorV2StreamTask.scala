package org.sunbird.job.task

import java.io.File
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.domain.Event
import org.sunbird.job.functions.AutoCreatorFunction
import org.sunbird.job.util.{FlinkUtil, HttpUtil}


class AutoCreatorV2StreamTask(config: AutoCreatorV2Config, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)).name(config.eventConsumer)
      .uid(config.eventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new AutoCreatorFunction(config, httpUtil))
      .name(config.autoCreatorV2Function)
      .uid(config.autoCreatorV2Function)
      .setParallelism(config.parallelism)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object AutoCreatorV2StreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("auto-creator-v2.conf").withFallback(ConfigFactory.systemEnvironment()))
    val autoCreatorConfig = new AutoCreatorV2Config(config)
    val kafkaUtil = new FlinkKafkaConnector(autoCreatorConfig)
    val httpUtil = new HttpUtil
    val task = new AutoCreatorV2StreamTask(autoCreatorConfig, kafkaUtil, httpUtil)
    task.process()
  }
}

// $COVERAGE-ON$
