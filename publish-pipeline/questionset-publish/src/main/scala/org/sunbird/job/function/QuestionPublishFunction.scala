package org.sunbird.job.function

import java.lang.reflect.Type

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.publish.domain.PublishMetadata
import org.sunbird.job.publish.helpers.QuestionPublisher
import org.sunbird.job.task.QuestionSetPublishConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}

class QuestionPublishFunction(config: QuestionSetPublishConfig, httpUtil: HttpUtil,
                              @transient var neo4JUtil: Neo4JUtil = null,
                              @transient var cassandraUtil: CassandraUtil = null)
                             (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[PublishMetadata, String](config) with QuestionPublisher {

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPublishFunction])
	val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
	val extProps = List("body", "editorState", "answer", "solutions", "instructions", "hints", "media","responseDeclaration", "interactions")

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
		List(config.questionPublishEventCount)
	}

	override def processElement(data: PublishMetadata, context: ProcessFunction[PublishMetadata, String]#Context, metrics: Metrics): Unit = {
		logger.info("Question publishing started for : " + data.identifier)
		val obj = getObject(data.identifier, data.pkgVersion)(neo4JUtil, cassandraUtil)
		val messages:List[String] = validate(obj, obj.identifier, validateQuestion)
		if (messages.isEmpty) {
			val enrichedObj = enrichObject(obj)(neo4JUtil)
			saveOnSuccess(enrichedObj, dummyFunc)(neo4JUtil)
			logger.info("Question publishing completed successfully for : " + data.identifier)
		} else {
			saveOnFailure(obj, messages)(neo4JUtil)
			logger.info("Question publishing failed for : " + data.identifier)
		}
	}

	override def getExtData(identifier: String)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		val row = getQuestionData(identifier)(cassandraUtil)
		val data = extProps.map(prop => prop -> row.getString(prop)).toMap
		if(null!=row) Option(data) else Option(Map[String, AnyRef]())
	}

	def getQuestionData(identifier: String)(implicit cassandraUtil: CassandraUtil) = {
		val select = QueryBuilder.select()
		extProps.foreach(prop => if(lang3.StringUtils.equals("body", prop) | lang3.StringUtils.equals("answer", prop)) select.fcall("blobAsText", QueryBuilder.column(prop)).as(prop) else select.column(prop).as(prop))
		val selectWhere: Select.Where = select.from(config.questionKeyspaceName, config.questionTableName).where()
		selectWhere.and(QueryBuilder.eq("identifier", identifier))
		//metrics.incCounter(config.dbReadCount)
		cassandraUtil.findOne(selectWhere.toString)
	}
}
