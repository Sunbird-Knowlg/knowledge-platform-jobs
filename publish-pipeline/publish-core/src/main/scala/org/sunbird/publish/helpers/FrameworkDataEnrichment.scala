package org.sunbird.publish.helpers

import java.util

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.util.Neo4JUtil
import org.sunbird.publish.core.ObjectData

import scala.collection.JavaConverters._

trait FrameworkDataEnrichment {

	private[this] val logger = LoggerFactory.getLogger(classOf[FrameworkDataEnrichment])

	private val fwMetaFields = List("boardIds", "subjectIds", "mediumIds", "topicsIds", "gradeLevelIds", "targetBoardIds", "targetSubjectIds", "targetMediumIds", "targetTopicIds", "targetGradeLevelIds")
	private val fwMetaMap = Map(("se_boardIds", "se_boards") -> List("boardIds", "targetBoardIds"), ("se_subjectIds", "se_subjects") -> List("subjectIds", "targetSubjectIds"), ("se_mediumIds", "se_mediums") -> List("mediumIds", "targetMediumIds"), ("se_topicIds", "se_topics") -> List("topicsIds", "targetTopicIds"), ("se_gradeLevelIds", "se_gradeLevels") -> List("gradeLevelIds", "targetGradeLevelIds"))

	def enrichFrameworkData(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil): ObjectData = {
		val enMetadata = enrichFwData(obj.identifier, obj.metadata)
		logger.info("Enriched Framework Metadata for " + obj.identifier + " are : " + enMetadata)
		val finalMeta = if(enMetadata.nonEmpty) obj.metadata ++ enMetadata else obj.metadata
		new ObjectData(obj.identifier, finalMeta, obj.extData, obj.hierarchy)
	}

	private def enrichFwData(identifier: String, metadata: Map[String, AnyRef])(implicit neo4JUtil: Neo4JUtil): Map[String, AnyRef] = {
		val mFwId = getList(metadata.getOrElse("framework", "")) ::: getList(metadata.getOrElse("targetFWIds", List()))
		if (mFwId.isEmpty) Map() else {
			val labels: Map[String, List[String]] = getLabels(identifier, metadata)
			val metaMap = fwMetaMap.flatMap(entry => Map(entry._1._1.toString -> (getList(metadata.getOrElse(entry._2(0), List())) ::: getList(metadata.getOrElse(entry._2(1), List()))).distinct, entry._1._2 -> (labels.getOrElse(entry._2(0), List()) ::: labels.getOrElse(entry._2(1), List())).distinct))
			(metaMap ++ Map("se_FWIds" -> mFwId)).filter(entry => entry._2.asInstanceOf[List[String]].nonEmpty)
		}
	}

	private def getLabels(identifier: String, metadata: Map[String, AnyRef])(implicit neo4JUtil: Neo4JUtil): Map[String, List[String]] = {
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

}
