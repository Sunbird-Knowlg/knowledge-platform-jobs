package org.sunbird.job.function

import java.lang.reflect.Type

import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.publish.domain.PublishMetadata
import org.sunbird.job.publish.helpers.QuestionSetPublisher
import org.sunbird.job.task.QuestionSetPublishConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}
import org.sunbird.publish.core.ExtDataConfig

class QuestionSetPublishFunction(config: QuestionSetPublishConfig, httpUtil: HttpUtil,
                                 @transient var neo4JUtil: Neo4JUtil = null,
                                 @transient var cassandraUtil: CassandraUtil = null)
                                (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[PublishMetadata, String](config) with QuestionSetPublisher {

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionSetPublishFunction])
	val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
	private val readerConfig = ExtDataConfig(config.questionSetKeyspaceName, config.questionSetTableName)

	override def open(parameters: Configuration): Unit = {
		super.open(parameters)
		cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort)
		neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
	}

	override def close(): Unit = {
		super.close()
		cassandraUtil.close()
	}

	override def metricsList(): List[String] = {
		List(config.questionSetPublishEventCount)
	}

	override def processElement(data: PublishMetadata, context: ProcessFunction[PublishMetadata, String]#Context, metrics: Metrics): Unit = {
		logger.info("QuestionSet publishing started for : " + data.identifier)
		val obj = getObject(data.identifier, data.pkgVersion, readerConfig)(neo4JUtil, cassandraUtil)
		val messages:List[String] = validate(obj, obj.identifier, validateQuestionSet)
		if (messages.isEmpty) {
			//TODO: enrichObject should take a function as parameter for enriching questionset hierarchy as it requires internal question publish
			val enrichedObj = enrichObject(obj)(neo4JUtil)
			//TODO: Implement the dummyFunc function to save hierarchy into cassandra.
			saveOnSuccess(enrichedObj, dummyFunc)(neo4JUtil)
			logger.info("QuestionSet publishing completed successfully for : " + data.identifier)
		} else {
			saveOnFailure(obj, messages)(neo4JUtil)
			logger.info("QuestionSet publishing failed for : " + data.identifier)
		}
	}

}
