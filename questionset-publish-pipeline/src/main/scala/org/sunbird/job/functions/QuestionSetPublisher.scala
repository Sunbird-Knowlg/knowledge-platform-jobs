package org.sunbird.job.functions

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.publish.domain.Event
import org.sunbird.job.publish.helpers.QuestionSetPublish
import org.sunbird.job.task.QuestionSetPublishPipelineConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}

class QuestionSetPublisher(config: QuestionSetPublishPipelineConfig, httpUtil: HttpUtil,
                           @transient var cassandraUtil: CassandraUtil = null,
                           @transient var neo4JUtil: Neo4JUtil = null)
  extends BaseProcessFunction[Event, String](config) with QuestionSetPublish {

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionSetPublisher])
	val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

	override def open(parameters: Configuration): Unit = {
		super.open(parameters)
		cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort)
		neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
	}

	override def close(): Unit = {
		cassandraUtil.close()
		super.close()
	}

	override def metricsList(): List[String] = {
		List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount)
	}

	override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
		logger.info("QuestionSetPublisher :: Processed event using JobRequest-SerDe: " + event)
		if (event.validEvent()) {
			val identifier = event.objectId
			val metadata = neo4JUtil.getNodeProperties(identifier)
			logger.info("QuestionSetPublisher :: Node Identifier ::: " + identifier)
			logger.info("QuestionSetPublisher :: Node Metadata ::: " + metadata)
			println("Node Identifier ::: " + identifier)
			println("Node Metadata ::: " + metadata)
			println("Is valid metadata ??? " + validateObject(metadata))
			// Validate Node Object
		} else {
			metrics.incCounter(config.skippedEventCount)
		}
		metrics.incCounter(config.totalEventsCount)
	}
}
