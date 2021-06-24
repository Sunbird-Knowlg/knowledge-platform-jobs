package org.sunbird.job.questionset.function

import akka.dispatch.ExecutionContexts
import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.util.CloudStorageUtil
import org.sunbird.job.questionset.publish.domain.PublishMetadata
import org.sunbird.job.questionset.publish.helpers.QuestionSetPublisher
import org.sunbird.job.questionset.publish.util.QuestionPublishUtil
import org.sunbird.job.questionset.task.QuestionSetPublishConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}
import java.lang.reflect.Type

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

class QuestionSetPublishFunction(config: QuestionSetPublishConfig, httpUtil: HttpUtil,
                                 @transient var neo4JUtil: Neo4JUtil = null,
                                 @transient var cassandraUtil: CassandraUtil = null,
                                 @transient var cloudStorageUtil: CloudStorageUtil = null,
                                 @transient var definitionCache: DefinitionCache = null,
                                 @transient var definitionConfig: DefinitionConfig = null)
                                (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[PublishMetadata, String](config) with QuestionSetPublisher {

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionSetPublishFunction])
	val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
	private val qReaderConfig = ExtDataConfig(config.questionKeyspaceName, config.questionTableName)

	@transient var ec: ExecutionContext = _
	private val pkgTypes = List("SPINE", "ONLINE")


	override def open(parameters: Configuration): Unit = {
		super.open(parameters)
		cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort)
		neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
		cloudStorageUtil = new CloudStorageUtil(config)
		ec = ExecutionContexts.global
		definitionCache = new DefinitionCache()
		definitionConfig = DefinitionConfig(config.schemaSupportVersionMap, config.definitionBasePath)
	}

	override def close(): Unit = {
		super.close()
		cassandraUtil.close()
	}

	override def metricsList(): List[String] = {
		List(config.questionSetPublishEventCount, config.questionSetPublishSuccessEventCount, config.questionSetPublishFailedEventCount)
	}

	override def processElement(data: PublishMetadata, context: ProcessFunction[PublishMetadata, String]#Context, metrics: Metrics): Unit = {
		logger.info("QuestionSet publishing started for : " + data.identifier)
		metrics.incCounter(config.questionSetPublishEventCount)
		val definition: ObjectDefinition = definitionCache.getDefinition(data.objectType, config.schemaSupportVersionMap.getOrElse(data.objectType.toLowerCase(), "1.0").asInstanceOf[String], config.definitionBasePath)
		val readerConfig = ExtDataConfig(config.questionSetKeyspaceName, config.questionSetTableName, definition.getExternalPrimaryKey, definition.getExternalProps)
		val obj = getObject(data.identifier, data.pkgVersion, readerConfig)(neo4JUtil, cassandraUtil)
		logger.info("processElement ::: obj metadata before publish ::: " + ScalaJsonUtil.serialize(obj.metadata))
		logger.info("processElement ::: obj hierarchy before publish ::: " + ScalaJsonUtil.serialize(obj.hierarchy.getOrElse(Map())))
		val messages: List[String] = validate(obj, obj.identifier, validateQuestionSet)
		if (messages.isEmpty) {
			// Get all the questions from hierarchy
			val qList: List[ObjectData] = getQuestions(obj, qReaderConfig)(cassandraUtil)
			logger.info("processElement ::: child questions list from hierarchy :::  " + qList)
			// Filter out questions having visibility parent (which need to be published)
			val childQuestions: List[ObjectData] = qList.filter(q => isValidChildQuestion(q))
			//TODO: Remove below statement
			childQuestions.foreach(ch => logger.info("child questions visibility parent identifier : " + ch.identifier))
			// Publish Child Questions
			QuestionPublishUtil.publishQuestions(obj.identifier, childQuestions)(ec, neo4JUtil, cassandraUtil, qReaderConfig, cloudStorageUtil, definitionCache, definitionConfig, config)
			val pubMsgs: List[String] = isChildrenPublished(childQuestions, qReaderConfig)
			if(pubMsgs.isEmpty) {
				// Enrich Object as well as hierarchy
				val enrichedObj = enrichObject(obj)(neo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, config)
				logger.info(s"processElement ::: object enrichment done for ${obj.identifier}")
				logger.info("processElement :::  obj metadata post enrichment :: " + ScalaJsonUtil.serialize(enrichedObj.metadata))
				logger.info("processElement :::  obj hierarchy post enrichment :: " + ScalaJsonUtil.serialize(enrichedObj.hierarchy.get))
				// Generate ECAR
				val objWithEcar = generateECAR(enrichedObj, pkgTypes)(ec, cloudStorageUtil, definitionCache, definitionConfig)
				// Generate PDF URL
				val updatedObj = generatePreviewUrl(objWithEcar, qList)(httpUtil, cloudStorageUtil)
				saveOnSuccess(updatedObj)(neo4JUtil, cassandraUtil, readerConfig, definitionCache, definitionConfig)
				logger.info("QuestionSet publishing completed successfully for : " + data.identifier)
				metrics.incCounter(config.questionSetPublishSuccessEventCount)
			} else {
				saveOnFailure(obj, pubMsgs)(neo4JUtil)
				metrics.incCounter(config.questionSetPublishFailedEventCount)
				logger.info("QuestionSet publishing failed for : " + data.identifier)
			}
		} else {
			saveOnFailure(obj, messages)(neo4JUtil)
			metrics.incCounter(config.questionSetPublishFailedEventCount)
			logger.info("QuestionSet publishing failed for : " + data.identifier)
		}
	}

	//TODO: Implement Multiple Data Read From Neo4j and Use it here.
	def isChildrenPublished(children: List[ObjectData], readerConfig: ExtDataConfig): List[String] = {
		val messages = ListBuffer[String]()
		children.foreach(q => {
			val id = q.identifier.replace(".img", "")
			val obj = getObject(id, 0, readerConfig)(neo4JUtil, cassandraUtil)
			logger.info(s"question metadata for $id : ${obj.metadata}")
			if (!List("Live", "Unlisted").contains(obj.metadata.getOrElse("status", "").asInstanceOf[String])) {
				logger.info("Question publishing failed for : " + id)
				messages += s"""Question publishing failed for : $id"""
			}
		})
		messages.toList
	}


	def generateECAR(data: ObjectData, pkgTypes: List[String])(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil, defCache: DefinitionCache, defConfig: DefinitionConfig): ObjectData = {
		val ecarMap: Map[String, String] = generateEcar(data, pkgTypes)
		val variants: java.util.Map[String, String] = ecarMap.map { case (key, value) => key.toLowerCase -> value }.asJava
		logger.info("QuestionSetPublishFunction ::: generateECAR ::: ecar map ::: " + ecarMap)
		val meta: Map[String, AnyRef] = Map("downloadUrl" -> ecarMap.getOrElse("SPINE", ""), "variants" -> variants)
		new ObjectData(data.identifier, data.metadata ++ meta, data.extData, data.hierarchy)
	}

	def generatePreviewUrl(data: ObjectData, qList: List[ObjectData])(implicit httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): ObjectData = {
		val (pdfUrl, previewUrl) = getPdfFileUrl(qList, data, "questionSetTemplate.vm", config.printServiceBaseUrl, System.currentTimeMillis().toString)(httpUtil, cloudStorageUtil)
		logger.info("generatePreviewUrl ::: finalPdfUrl ::: " + pdfUrl.getOrElse(""))
		logger.info("generatePreviewUrl ::: finalPreviewUrl ::: " + previewUrl.getOrElse(""))
		new ObjectData(data.identifier, data.metadata ++ Map("previewUrl" -> previewUrl.getOrElse(""), "pdfUrl" -> pdfUrl.getOrElse("")), data.extData, data.hierarchy)
	}

	def isValidChildQuestion(obj: ObjectData): Boolean = {
		StringUtils.equalsIgnoreCase("Parent", obj.metadata.getOrElse("visibility", "").asInstanceOf[String])
	}

}
