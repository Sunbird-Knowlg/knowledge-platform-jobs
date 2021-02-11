package org.sunbird.job.publish.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.apache.commons.lang3
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil, ScalaJsonUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}
import org.sunbird.publish.helpers.{EcarGenerator, ObjectEnrichment, ObjectReader, ObjectUpdater, ObjectValidator, QuestionPdfGenerator}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

trait QuestionSetPublisher extends ObjectReader with ObjectValidator with ObjectUpdater with ObjectEnrichment with EcarGenerator with QuestionPdfGenerator {

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionSetPublisher])
	val extProps = List("body", "editorState", "answer", "solutions", "instructions", "hints", "media", "responseDeclaration", "interactions", "identifier")

	override def getExtData(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

	def validateQuestionSet(obj: ObjectData, identifier: String): List[String] = {
		val messages = ListBuffer[String]()
		if (obj.hierarchy.getOrElse(Map()).isEmpty) messages += s"""There is no hierarchy available for : $identifier"""
		if (!StringUtils.equalsIgnoreCase(obj.metadata.getOrElse("mimeType", "").asInstanceOf[String], "application/vnd.sunbird.questionset"))
			messages += s"""mimeType is invalid for : $identifier"""
		if (obj.metadata.getOrElse("visibility", "").asInstanceOf[String].isEmpty) messages += s"""There is no visibility available for : $identifier"""
		//TODO: Add any more check, if required.
		messages.toList
	}

	override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		val row: Row = Option(getQuestionSetHierarchy(getEditableObjId(identifier, pkgVersion), readerConfig)).getOrElse(getQuestionSetHierarchy(identifier, readerConfig))
		if (null != row) {
			val data: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](row.getString("hierarchy"))
			Option(data)
		} else Option(Map())
	}

	override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		val rows = getQuestionsExtData(identifiers, readerConfig)(cassandraUtil).asScala
		if (rows.nonEmpty)
			Option(rows.map(row => row.getString("identifier") -> extProps.map(prop => (prop -> row.getString(prop))).toMap).toMap)
		else
			Option(Map[String, AnyRef]())
	}

	override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		None
	}

	def getQuestionsExtData(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
		logger.info("QuestionSetPublisher ::: getQuestionsExtData ::: reader config ::: keyspace: " + readerConfig.keyspace + " ,  table : " + readerConfig.table)
		val select = QueryBuilder.select()
		extProps.foreach(prop => if (lang3.StringUtils.equals("body", prop) | lang3.StringUtils.equals("answer", prop)) select.fcall("blobAsText", QueryBuilder.column(prop)).as(prop) else select.column(prop).as(prop))
		val selectWhere: Select.Where = select.from(readerConfig.keyspace, readerConfig.table).where()
		selectWhere.and(QueryBuilder.in("identifier", identifiers.asJava))
		logger.info("QuestionSetPublisher ::: getQuestionsExtData ::: cassandra query ::: " + selectWhere.toString)
		cassandraUtil.find(selectWhere.toString)
	}


	def getQuestionSetHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
		val selectWhere: Select.Where = QueryBuilder.select().all()
		  .from(readerConfig.keyspace, readerConfig.table).
		  where()
		selectWhere.and(QueryBuilder.eq("identifier", identifier))
		cassandraUtil.findOne(selectWhere.toString)
	}

	override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
		val identifier = obj.identifier.replace(".img", "")
		val children: List[Map[String, AnyRef]] = obj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
		val hierarchy: Map[String, AnyRef] = obj.metadata ++ Map("children" -> children)
		val query = QueryBuilder.update(readerConfig.keyspace, readerConfig.table)
		  .`with`(QueryBuilder.set("hierarchy", ScalaJsonUtil.serialize(hierarchy).toString))
		  .where(QueryBuilder.eq("identifier", identifier))
		logger.info(s"Update Hierarchy Query For $identifier : ${query.toString}")
		val result = cassandraUtil.upsert(query.toString)
		if (result) {
			logger.info(s"Hierarchy Updated Successfully For $identifier")
		} else {
			val msg = s"Hierarchy Update Failed For $identifier"
			logger.error(msg)
			throw new Exception(msg)
		}
	}

	def getQuestions(qsObj: ObjectData, readerConfig: ExtDataConfig)
	                (implicit cassandraUtil: CassandraUtil): List[ObjectData] = {
		val childrenMaps: Map[String, AnyRef] = populateChildrenMapRecursively(qsObj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]], Map())
		logger.info("QuestionSetPublisher ::: getQuestions ::: child questions  ::::: " + childrenMaps)
		val extMap = getExtDatas(childrenMaps.keys.toList, readerConfig)
		logger.info("QuestionSetPublisher ::: getQuestions ::: child questions external data ::::: " + extMap.get)
		extMap.getOrElse(Map()).map(entry => {
			val metadata = childrenMaps.getOrElse(entry._1, Map()).asInstanceOf[Map[String, AnyRef]] ++ Map("IL_UNIQUE_ID" -> entry._1)
			val obj = new ObjectData(entry._1, metadata, Option(entry._2.asInstanceOf[Map[String, AnyRef]]))
			obj
		}).toList
	}


	def populateChildrenMapRecursively(children: List[Map[String, AnyRef]], childrenMap: Map[String, AnyRef]): Map[String, AnyRef] = {
		val result = children.flatMap(child => {
			val updatedChildrenMap: Map[String, AnyRef] =
				if (child.getOrElse("objectType", "").asInstanceOf[String].equalsIgnoreCase("Question")) {
					Map(child.getOrElse("identifier", "").asInstanceOf[String] -> child) ++ childrenMap
				} else childrenMap
			val nextChild: List[Map[String, AnyRef]] = child.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
			val map = populateChildrenMapRecursively(nextChild, updatedChildrenMap)
			map ++ updatedChildrenMap
		}).toMap
		result
	}

	override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = {
		val hChildren: List[Map[String, AnyRef]] = obj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
		Some(getFlatStructure(List(obj.metadata ++ Map("children" -> hChildren)), List()))
	}

	def getFlatStructure(children: List[Map[String, AnyRef]], childrenList: List[Map[String, AnyRef]]): List[Map[String, AnyRef]] = {
		children.flatMap(child => {
			val innerChildren = getInnerChildren(child)
			val updatedChild: Map[String, AnyRef] = if (innerChildren.nonEmpty) child ++ Map("children" -> innerChildren) else child
			val finalChild = updatedChild.filter(p => !excludeBundleMeta.contains(p._1.asInstanceOf[String]))
			val updatedChildren: List[Map[String, AnyRef]] = finalChild :: childrenList
			val result = getFlatStructure(child.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]], updatedChildren)
			finalChild :: result
		}).distinct
	}

	def getInnerChildren(child: Map[String, AnyRef]): List[Map[String, AnyRef]] = {
		val metaList: List[String] = List("identifier", "name", "objectType", "description", "index")
		child.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
		  .map(ch => ch.filterKeys(key => metaList.contains(key)))
	}

	override def enrichObjectMetadata(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): Option[ObjectData] = {
		val newMetadata: Map[String, AnyRef] = obj.metadata ++ Map("pkgVersion" -> (obj.pkgVersion + 1).asInstanceOf[AnyRef], "lastPublishedOn" -> getTimeStamp,
			"publishError" -> null, "variants" -> null, "compatibilityLevel" -> 5.asInstanceOf[AnyRef], "status" -> "Live")
		val children: List[Map[String, AnyRef]] = obj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
		Some(new ObjectData(obj.identifier, newMetadata, hierarchy = Some(Map("identifier" -> obj.identifier, "children" -> enrichChildren(children)))))
	}

	def enrichChildren(children: List[Map[String, AnyRef]])(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): List[Map[String, AnyRef]] = {
		val newChildren = children.map(element => enrichMetadata(element))
		newChildren
	}

	def enrichMetadata(element: Map[String, AnyRef])(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): Map[String, AnyRef] = {
		if (StringUtils.equalsIgnoreCase(element.getOrElse("objectType", "").asInstanceOf[String], "QuestionSet")
		  && StringUtils.equalsIgnoreCase(element.getOrElse("visibility", "").asInstanceOf[String], "Parent")) {
			val children: List[Map[String, AnyRef]] = element.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
			val enrichedChildren = enrichChildren(children)
			element ++ Map("children" -> enrichedChildren, "status" -> "Live")
		} else if (StringUtils.equalsIgnoreCase(element.getOrElse("objectType", "").toString, "QuestionSet")
		  && StringUtils.equalsIgnoreCase(element.getOrElse("visibility", "").toString, "Default")) {
			val childHierarchy: Map[String, AnyRef] = getHierarchy(element.getOrElse("identifier", "").toString, readerConfig).getOrElse(Map())
			childHierarchy ++ Map("index" -> element.getOrElse("index", 0).asInstanceOf[AnyRef], "depth" -> element.getOrElse("depth", 0).asInstanceOf[AnyRef], "parent" -> element.getOrElse("parent", ""))
		} else if (StringUtils.equalsIgnoreCase(element.getOrElse("objectType", "").toString, "Question")) {
			val newObject: ObjectData = getObject(element.getOrElse("identifier", "").toString, 0.asInstanceOf[Double], readerConfig)
			logger.info("enrichMeta :::: question object meta ::: " + newObject.metadata)
			newObject.metadata ++ Map("index" -> element.getOrElse("index", 0).asInstanceOf[AnyRef], "parent" -> element.getOrElse("parent", ""), "depth" -> element.getOrElse("depth", 0).asInstanceOf[AnyRef])
		} else Map()
	}

	override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = None

}
