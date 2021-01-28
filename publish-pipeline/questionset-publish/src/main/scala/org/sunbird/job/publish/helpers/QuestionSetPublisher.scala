package org.sunbird.job.publish.helpers

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.apache.commons.lang3
import org.apache.commons.lang3.StringUtils
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil, ScalaJsonUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}
import org.sunbird.publish.helpers.{EcarGenerator, ObjectEnrichment, ObjectReader, ObjectUpdater, ObjectValidator, QuestionPdfGenerator}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

trait QuestionSetPublisher extends ObjectReader with ObjectValidator with ObjectUpdater with ObjectEnrichment with EcarGenerator with QuestionPdfGenerator {
	val extProps = List("body", "editorState", "answer", "solutions", "instructions", "hints", "media", "responseDeclaration", "interactions", "identifier")

	override def getExtData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

	def validateQuestionSet(obj: ObjectData, identifier: String): List[String] = {
		val messages = ListBuffer[String]()
		if (obj.hierarchy.getOrElse(Map()).isEmpty) messages += s"""There is no hierarchy available for : $identifier"""
		//TODO: validate questionset specific metadata
		messages.toList
	}

	override def getHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		val row = getQuestionSetHierarchy(identifier, readerConfig)
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
		println("getQuestionsExtData :::: identifiers ::: " + identifiers)
		println("getQuestionsExtData :::: readerConfig ::: keyspace::: " + readerConfig.keyspace + " ,  table ::: " + readerConfig.table)
		val select = QueryBuilder.select()
		extProps.foreach(prop => if (lang3.StringUtils.equals("body", prop) | lang3.StringUtils.equals("answer", prop)) select.fcall("blobAsText", QueryBuilder.column(prop)).as(prop) else select.column(prop).as(prop))
		val selectWhere: Select.Where = select.from(readerConfig.keyspace, readerConfig.table).where()
		selectWhere.and(QueryBuilder.in("identifier", identifiers.asJava))
		println("select query ::: " + selectWhere.toString)
		cassandraUtil.find(selectWhere.toString)
	}


	def getQuestionSetHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
		val selectWhere: Select.Where = QueryBuilder.select().all()
		  .from(readerConfig.keyspace, readerConfig.table).
		  where()
		selectWhere.and(QueryBuilder.eq("identifier", identifier))
		cassandraUtil.findOne(selectWhere.toString)
	}

	def dummyFunc = (obj: ObjectData) => {}

	def getQuestions(qsObj: ObjectData, readerConfig: ExtDataConfig)
	                (implicit cassandraUtil: CassandraUtil): List[ObjectData] = {
		//val hierarchy = getHierarchy(questionSet.dbId, readerConfig).getOrElse(Map())
		val childrenMaps: Map[String, AnyRef] = populateChildrenMapRecursively(qsObj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]], Map())
		println("childrenMaps ::::: " + childrenMaps)
		val extMap = getExtDatas(childrenMaps.keys.toList, readerConfig)
		println("expMap :::: " + extMap.get)

		extMap.getOrElse(Map()).map(entry => {
			println("entry._1 :::: " + entry._1)
			println("childrenMaps.getOrElse(entry._1, Map()).asInstanceOf[Map[String, AnyRef]] :::: " + childrenMaps.getOrElse(entry._1, Map()).asInstanceOf[Map[String, AnyRef]])
			println("entry._2.asInstanceOf[Map[String, AnyRef]]) :::: " + entry._2.asInstanceOf[Map[String, AnyRef]])
			println()
			val metadata = childrenMaps.getOrElse(entry._1, Map()).asInstanceOf[Map[String, AnyRef]] ++ Map("IL_UNIQUE_ID" -> entry._1)
			val obj = new ObjectData(entry._1, metadata,
				Option(entry._2.asInstanceOf[Map[String, AnyRef]]))
			println("obj.metadata ::: " + obj.metadata)
			println("obj ext data ::: " + obj.extData.get)
			obj
		}).toList
	}

	/**
	 *
	 * @param children
	 * @param childrenMap
	 * @return a map of id and metadata map
	 */
	def populateChildrenMapRecursively(children: List[Map[String, AnyRef]], childrenMap: Map[String, AnyRef]): Map[String, AnyRef] = {
		println("call inside populateChildrenMapRecursively :::: children :::" + children)
		val result = children.flatMap(child => {
			println("child ::: " + child)
			val updatedChildrenMap: Map[String, AnyRef] =
				if (child.getOrElse("objectType", "").asInstanceOf[String].equalsIgnoreCase("Question")) {
					Map(child.getOrElse("identifier", "").asInstanceOf[String] -> child) ++ childrenMap
				} else childrenMap
			println("updated children Map ::: " + updatedChildrenMap)
			val nextChild: List[Map[String, AnyRef]] = child.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
			val map = populateChildrenMapRecursively(nextChild, updatedChildrenMap)
			println("map ::: " + map)
			map ++ updatedChildrenMap
		}).toMap
		println("result ::: " + result)
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
			val finalChild = updatedChild.filter(p => !List("index", "depth").contains(p._1.asInstanceOf[String]))
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
		println("element: *" + element.getOrElse("objectType", "").asInstanceOf[String] + "*")
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
			newObject.metadata ++ Map("index" -> element.getOrElse("index", 0).asInstanceOf[AnyRef], "parent" -> element.getOrElse("parent", ""), "depth" -> element.getOrElse("depth", 0).asInstanceOf[AnyRef])
		} else Map()
	}

}
