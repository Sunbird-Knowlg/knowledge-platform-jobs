package org.sunbird.job.publish.helpers

import java.util

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.apache.commons.lang3
import org.slf4j.LoggerFactory
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}
import org.sunbird.publish.helpers.{ObjectEnrichment, ObjectReader, ObjectUpdater, ObjectValidator}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

trait QuestionPublisher extends ObjectReader with ObjectValidator with ObjectEnrichment with ObjectUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPublisher])
	val extProps = List("body", "editorState", "answer", "solutions", "instructions", "hints", "media","responseDeclaration", "interactions")

	override def getHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

	override def enrichObjectMetadata(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): Option[ObjectData] = None

	def validateQuestion(obj: ObjectData, identifier: String): List[String] = {
		logger.info("Validating Question External Data For : "+obj.identifier)
		//TODO: Remove Below logger statement
		logger.info("Question External Data : "+obj.extData.get)
		val messages = ListBuffer[String]()
		if (obj.extData.get.getOrElse("body", "").asInstanceOf[String].isEmpty) messages += s"""There is no body available for : $identifier"""
		if (obj.extData.get.getOrElse("editorState", "").asInstanceOf[String].isEmpty) messages += s"""There is no editorState available for : $identifier"""
		val interactionTypes = obj.metadata.getOrElse("interactionTypes", new util.ArrayList[String]()).asInstanceOf[util.List[String]].asScala.toList
		interactionTypes.nonEmpty match {
			case true => {
				if (obj.extData.get.getOrElse("responseDeclaration", "").asInstanceOf[String].isEmpty) messages += s"""There is no responseDeclaration available for : $identifier"""
				if (obj.extData.get.getOrElse("interactions", "").asInstanceOf[String].isEmpty) messages += s"""There is no interactions available for : $identifier"""
			}
			case false => if (obj.extData.getOrElse("answer", "").toString.isEmpty) messages += s"""There is no answer available for : $identifier"""
		}
		messages.toList
	}

	override def getExtData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		val row = getQuestionData(identifier, readerConfig)(cassandraUtil)
		//TODO: covert below code in scala. entrire props should be in scala.
		if(null!=row) Option(extProps.map(prop => prop -> row.getString(prop)).toMap) else Option(Map[String, AnyRef]())
	}

	def getQuestionData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
		val select = QueryBuilder.select()
		extProps.foreach(prop => if(lang3.StringUtils.equals("body", prop) | lang3.StringUtils.equals("answer", prop)) select.fcall("blobAsText", QueryBuilder.column(prop)).as(prop) else select.column(prop).as(prop))
		val selectWhere: Select.Where = select.from(readerConfig.keyspace, readerConfig.table).where()
		selectWhere.and(QueryBuilder.eq("identifier", identifier))
		cassandraUtil.findOne(selectWhere.toString)
	}

	def dummyFunc = (obj: ObjectData) => {}

	override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		None
	}

	override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		None
	}
}
