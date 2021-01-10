package org.sunbird.job.function

import java.lang.reflect.Type

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.publish.domain.PublishMetadata
import org.sunbird.job.publish.helpers.QuestionSetPublisher
import org.sunbird.job.task.QuestionSetPublishConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil, Neo4JUtil}

import scala.collection.JavaConverters._

class QuestionSetPublishFunction(config: QuestionSetPublishConfig, httpUtil: HttpUtil,
                                 @transient var neo4JUtil: Neo4JUtil = null,
                                 @transient var cassandraUtil: CassandraUtil = null)
                                (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[PublishMetadata, String](config) with QuestionSetPublisher {

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionSetPublishFunction])
	val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

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
		val obj = getObject(data.identifier, data.pkgVersion)(neo4JUtil, cassandraUtil)
		val messages:List[String] = validate(obj, obj.identifier, validateQuestionSet)
		if (messages.isEmpty) {
			//TODO: enrichObject should take a function as parameter for enriching questionset hierarchy
			// as it requires internal question publish
			val enrichedObj = enrichObject(obj)(neo4JUtil)
			//TODO: Implement the dummyFunc function to save hierarchy into cassandra.
			saveOnSuccess(enrichedObj, dummyFunc)(neo4JUtil)
			logger.info("QuestionSet publishing completed successfully for : " + data.identifier)
		} else {
			saveOnFailure(obj, messages)(neo4JUtil)
			logger.info("QuestionSet publishing failed for : " + data.identifier)
		}
	}

	override def getHierarchy(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		val row = getQuestionSetHierarchy(identifier)
		if (null != row) {
			val data = JSONUtil.deserialize[java.util.Map[String, AnyRef]](row.getString("hierarchy"))
			Option(data.asScala.toMap)
		} else Option(Map())
	}

	def getQuestionSetHierarchy(identifier: String)(implicit cassandraUtil: CassandraUtil) = {
		val selectWhere: Select.Where = QueryBuilder.select().all()
		  .from(config.questionSetKeyspaceName, config.questionSetTableName).
		  where()
		selectWhere.and(QueryBuilder.eq("identifier", identifier))
		//metrics.incCounter(config.dbReadCount)
		cassandraUtil.findOne(selectWhere.toString)
	}
}
