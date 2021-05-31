package org.sunbird.job.publish.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.util.Neo4JUtil
import java.util

import org.apache.commons.collections.CollectionUtils
import org.sunbird.job.cache.local.FrameworkMasterCategoryMap

import scala.collection.JavaConverters._

trait FrameworkDataEnrichment {

	private[this] val logger = LoggerFactory.getLogger(classOf[FrameworkDataEnrichment])

	//private val fwMetaFields = List("boardIds", "subjectIds", "mediumIds", "topicsIds", "gradeLevelIds", "targetBoardIds", "targetSubjectIds", "targetMediumIds", "targetTopicIds", "targetGradeLevelIds")
	//private val fwMetaMap = Map(("se_boardIds", "se_boards") -> List("boardIds", "targetBoardIds"), ("se_subjectIds", "se_subjects") -> List("subjectIds", "targetSubjectIds"), ("se_mediumIds", "se_mediums") -> List("mediumIds", "targetMediumIds"), ("se_topicIds", "se_topics") -> List("topicsIds", "targetTopicIds"), ("se_gradeLevelIds", "se_gradeLevels") -> List("gradeLevelIds", "targetGradeLevelIds"))

	def enrichFrameworkData(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil): ObjectData = {
		val (fwMetaFields, fwMetaMap) : (List[String], Map[(String, String), List[String]]) = getFrameworkCategoryMetadata("domain", "Category")
		val enMetadata = enrichFwData(obj.identifier, obj.metadata, fwMetaFields, fwMetaMap)
		logger.info("Enriched Framework Metadata for " + obj.identifier + " are : " + enMetadata)
		val finalMeta = if(enMetadata.nonEmpty) obj.metadata ++ enMetadata else obj.metadata
		new ObjectData(obj.identifier, finalMeta, obj.extData, obj.hierarchy)
	}

	private def enrichFwData(identifier: String, metadata: Map[String, AnyRef], fwMetaFields: List[String], fwMetaMap: Map[(String, String), List[String]])(implicit neo4JUtil: Neo4JUtil): Map[String, AnyRef] = {
		val mFwId = getList(metadata.getOrElse("framework", "")) ::: getList(metadata.getOrElse("targetFWIds", List()))
		if (mFwId.isEmpty) Map() else {
			val labels: Map[String, List[String]] = getLabels(identifier, metadata, fwMetaFields)
			val metaMap = fwMetaMap.flatMap(entry => Map(entry._1._1.toString -> (getList(metadata.getOrElse(entry._2(0), List())) ::: getList(metadata.getOrElse(entry._2(1), List()))).distinct, entry._1._2 -> (labels.getOrElse(entry._2(0), List()) ::: labels.getOrElse(entry._2(1), List())).distinct))
			(metaMap ++ Map("se_FWIds" -> mFwId)).filter(entry => entry._2.asInstanceOf[List[String]].nonEmpty)
		}
	}

	private def getLabels(identifier: String, metadata: Map[String, AnyRef], fwMetaFields: List[String])(implicit neo4JUtil: Neo4JUtil): Map[String, List[String]] = {
		val fwMetaIds = fwMetaFields.flatMap(meta => getList(metadata.getOrElse(meta, List())))
		if (fwMetaIds.isEmpty) {
			logger.info("No framework categories are present for identifier : " + identifier)
			Map()
		} else {
			val nameMap: Map[String, String] = neo4JUtil.getNodesName(fwMetaIds)
			if (nameMap.isEmpty) Map() else {
				fwMetaFields.flatMap(meta => {
					val metaNames: List[String] = getList(metadata.getOrElse(meta, List())).map(id => nameMap.getOrElse(id, ""))
					Map(meta -> metaNames)
				}).toMap
			}
		}
	}

	private def getList(obj: AnyRef): List[String] = {
		(obj match {
			case obj: List[String] => obj.distinct
			case obj: String => List(obj).distinct
			case obj: util.List[String] => obj.asScala.toList.distinct
			case _ => List.empty
		}).filter((x: String) => StringUtils.isNotBlank(x) && !StringUtils.equals(" ", x))
	}

	def getFrameworkCategoryMetadata(graphId: String, objectType: String)(implicit neo4JUtil: Neo4JUtil): (List[String], Map[(String, String), List[String]]) ={
		val masterCategories: List[Map[String, AnyRef]] = getMasterCategory(graphId, objectType)
		val fwMetaFields: List[String] = masterCategories.flatMap(category =>
			List(category.getOrElse("orgIdFieldName", "").asInstanceOf[String],
				category.getOrElse("targetIdFieldName", "").asInstanceOf[String]))

		val fwMetaMap: Map[(String, String), List[String]] = masterCategories.map(category =>
			(category.getOrElse("searchIdFieldName", "").asInstanceOf[String], category.getOrElse("searchLabelFieldName", "").asInstanceOf[String]) ->
				List(category.getOrElse("orgIdFieldName", "").asInstanceOf[String], category.getOrElse("targetIdFieldName", "").asInstanceOf[String])).toMap
		(fwMetaFields, fwMetaMap)
	}


	def getMasterCategory(graphId: String, objectType: String)(implicit neo4JUtil: Neo4JUtil): List[Map[String, AnyRef]] = {
		if (FrameworkMasterCategoryMap.containsKey("masterCategories") && null != FrameworkMasterCategoryMap.get("masterCategories")) {
			val masterCategories: Map[String, AnyRef] = FrameworkMasterCategoryMap.get("masterCategories")
			masterCategories.map(obj => obj._2.asInstanceOf[Map[String, AnyRef]]).toList
		}else{
			val nodes: util.List[util.Map[String, AnyRef]] = neo4JUtil.getNodePropertiesWithObjectType(objectType)
			if(CollectionUtils.isEmpty(nodes)){
				logger.info("No Framework Master Category found.")
				List()
			}else{
				val masterCategories: List[Map[String, AnyRef]] = nodes.asScala.map(node =>
					Map("code" -> node.getOrDefault("code", "").asInstanceOf[String],
						"orgIdFieldName" -> node.getOrDefault("orgIdFieldName", "").asInstanceOf[String],
						"targetIdFieldName" -> node.getOrDefault("targetIdFieldName", "").asInstanceOf[String],
						"searchIdFieldName" -> node.getOrDefault("searchIdFieldName", "").asInstanceOf[String],
						"searchLabelFieldName" -> node.getOrDefault("searchLabelFieldName", "").asInstanceOf[String])
				).toList
				masterCategories
			}

		}
	}


	/*def getMasterCategory(graphId: String, objectType: String)(implicit neo4JUtil: Neo4JUtil): (List[String], Map[(String, String), List[String]]) = {
		val nodes: util.List[util.Map[String, AnyRef]] = neo4JUtil.getNodePropertiesWithObjectType(objectType)
		nodes.forEach(node => println("node: " + node.get("code")))
		if(CollectionUtils.isEmpty(nodes)){
			(List(), Map())
		}

		val fwMetaFields: List[String] = nodes.asScala.flatMap(node =>
			List(node.get("orgIdFieldName").asInstanceOf[String],
				node.get("targetIdFieldName").asInstanceOf[String])).toList

		val fwMetaMap: Map[(String, String), List[String]] = nodes.asScala.map(node =>
			(node.get("searchIdFieldName").asInstanceOf[String], node.get("searchLabelFieldName").asInstanceOf[String]) ->
				List(node.get("orgIdFieldName").asInstanceOf[String], node.get("targetIdFieldName").asInstanceOf[String])).toMap
		(fwMetaFields, fwMetaMap)
	}*/
}
