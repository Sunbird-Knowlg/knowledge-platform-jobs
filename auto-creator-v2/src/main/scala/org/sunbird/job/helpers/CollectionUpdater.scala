package org.sunbird.job.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.util.{HTTPResponse, HttpUtil, JSONUtil}

trait CollectionUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[CollectionUpdater])

	def linkCollection(identifier: String, collection: List[Map[String, AnyRef]])(implicit config: AutoCreatorV2Config, httpUtil: HttpUtil) = {
		if (collection.nonEmpty)
			collection.foreach(coll => {
				val collId = coll.getOrElse("identifier", "").asInstanceOf[String]
				val unitId = coll.getOrElse("unitId", "").asInstanceOf[String]
				if ((StringUtils.isNotBlank(collId) && StringUtils.isNotBlank(unitId)) && isValidHierarchy(collId, unitId)) addToHierarchy(collId, unitId, identifier)
			})
	}

	def isValidHierarchy(collId: String, unitId: String)(implicit config: AutoCreatorV2Config, httpUtil: HttpUtil): Boolean = {
		val hierarchy = getHierarchy(collId)
		val childNodes: List[String] = hierarchy.getOrElse("childNodes", List()).asInstanceOf[List[String]]
		if (childNodes.nonEmpty && childNodes.contains(unitId)) true else false
	}

	def getHierarchy(identifier: String)(implicit config: AutoCreatorV2Config, httpUtil: HttpUtil): Map[String, AnyRef] = {
		val url = s"${config.contentServiceBaseUrl}/content/v3/hierarchy/$identifier?mode=edit"
		val resp: HTTPResponse = httpUtil.get(url)
		if (null != resp && resp.status == 200) getResult(resp).getOrElse("content", Map()).asInstanceOf[Map[String, AnyRef]] else {
			val msg = s"Unable to fetch collection hierarchy for : $identifier | Response Code : ${resp.status}"
			logger.error(msg)
			throw new Exception(msg)
		}
	}

	def addToHierarchy(collId: String, unitId: String, resourceId: String)(implicit config: AutoCreatorV2Config, httpUtil: HttpUtil) = {
		val url = config.contentServiceBaseUrl + "/content/v3/hierarchy/add"
		val requestBody = s"""{"request":{"rootId": "$collId", "unitId": "$unitId", "children": ["$resourceId"]}}"""
    logger.info(s"Add to hierarchy request body:" + ${requestBody})
		val resp = httpUtil.patch(url, requestBody)
		if (null != resp && resp.status == 200) {
			val contentId = getResult(resp).getOrElse("rootId", "").asInstanceOf[String]
			if (StringUtils.equalsIgnoreCase(contentId, collId))
				logger.info(s"Content Hierarchy Updated Successfully for: $collId")
		} else {
			val msg = s"Hierarchy Update Failed For : $collId. ${resp.body} :: status: ${resp.status}"
			logger.error(msg)
			throw new Exception(msg)
		}
	}

	def getResult(response: HTTPResponse): Map[String, AnyRef] = {
		val body = JSONUtil.deserialize[Map[String, AnyRef]](response.body)
		body.getOrElse("result", Map()).asInstanceOf[Map[String, AnyRef]]
	}
}
