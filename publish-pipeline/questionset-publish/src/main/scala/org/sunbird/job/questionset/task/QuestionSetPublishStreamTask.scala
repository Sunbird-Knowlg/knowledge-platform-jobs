package org.sunbird.job.questionset.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.questionset.function.{PublishEventRouter, QuestionPublishFunction, QuestionSetPublishFunction}
import org.sunbird.job.questionset.publish.domain.{Event, PublishMetadata}
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File
import java.util

class QuestionSetPublishStreamTask(config: QuestionSetPublishConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

	def process(): Unit = {
		implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
		implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
		implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
		implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
		implicit val publishMetaTypeInfo: TypeInformation[PublishMetadata] = TypeExtractor.getForClass(classOf[PublishMetadata])

		val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
		val processStreamTask = env.addSource(source).name(config.inputConsumerName)
		  .uid(config.inputConsumerName).setParallelism(config.kafkaConsumerParallelism)
		  .rebalance
		  .process(new PublishEventRouter(config))
		  .name("publish-event-router").uid("publish-event-router")
		  .setParallelism(config.eventRouterParallelism)

		processStreamTask.getSideOutput(config.questionPublishOutTag).process(new QuestionPublishFunction(config, httpUtil))
		  .name("question-publish-process").uid("question-publish-process").setParallelism(1)

		processStreamTask.getSideOutput(config.questionSetPublishOutTag).process(new QuestionSetPublishFunction(config, httpUtil))
		  .name("questionset-publish-process").uid("questionset-publish-process").setParallelism(1)
		env.execute(config.jobName)
	}
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object QuestionSetPublishStreamTask {

	def main(args: Array[String]): Unit = {
		val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
		val config = configFilePath.map {
			path => ConfigFactory.parseFile(new File(path)).resolve()
		}.getOrElse(ConfigFactory.load("questionset-publish.conf").withFallback(ConfigFactory.systemEnvironment()))
		val publishConfig = new QuestionSetPublishConfig(config)
		val kafkaUtil = new FlinkKafkaConnector(publishConfig)
		val httpUtil = new HttpUtil
		val task = new QuestionSetPublishStreamTask(publishConfig, kafkaUtil, httpUtil)
		task.process()
	}
}
