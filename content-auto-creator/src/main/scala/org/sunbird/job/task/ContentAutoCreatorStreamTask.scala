package org.sunbird.job.task

import java.io.File
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.util.{FlinkUtil, HttpUtil}
import org.slf4j.LoggerFactory
import org.sunbird.job.autocreatorv2.domain.Event
import org.sunbird.job.autocreatorv2.functions.{AutoCreatorFunction, LinkCollectionFunction}
import org.sunbird.job.autocreatorv2.model.ObjectParent


class ContentAutoCreatorStreamTask(config: AutoCreatorV2Config, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {
  private[this] val logger = LoggerFactory.getLogger(classOf[ContentAutoCreatorStreamTask])

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val objectParentTypeInfo: TypeInformation[ObjectParent] = TypeExtractor.getForClass(classOf[ObjectParent])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val contentAutoCreatorStream = env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)).name(config.eventConsumer)
      .uid(config.eventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new AutoCreatorFunction(config, httpUtil))
      .name(config.contentAutoCreatorFunction)
      .uid(config.contentAutoCreatorFunction)
      .setParallelism(config.parallelism)

    contentAutoCreatorStream.getSideOutput(config.contentAutoCreatorOutputTag).process(new ContentAutoCreatorFunction(config, httpUtil))
      .name(config.contentAutoCreatorFunction).uid(config.contentAutoCreatorFunction).setParallelism(config.contentAutoCreatorParallelism)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object ContentAutoCreatorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("content-auto-creator.conf").withFallback(ConfigFactory.systemEnvironment()))
    val contentAutoCreatorConfig = new ContentAutoCreatorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(contentAutoCreatorConfig)
    val httpUtil = new HttpUtil
    val task = new ContentAutoCreatorStreamTask(contentAutoCreatorConfig, kafkaUtil, httpUtil)
    task.process()
  }
}

// $COVERAGE-ON$
