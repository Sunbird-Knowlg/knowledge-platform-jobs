package org.sunbird.job.contentautocreator.helpers

import kong.unirest.Unirest
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.contentautocreator.domain.Event
import org.sunbird.job.contentautocreator.model.{ExtDataConfig, ObjectData}
import org.sunbird.job.contentautocreator.util.ContentAutoCreatorConstants
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.task.ContentAutoCreatorConfig
import org.sunbird.job.util._

import java.io.File
import java.util
import scala.collection.convert.ImplicitConversions.`map AsJavaMap`
import scala.collection.mutable

trait ContentAutoCreator extends ObjectUpdater with ContentCollectionUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[ContentAutoCreator])

	def process(config: ContentAutoCreatorConfig, event: Event, httpUtil: HttpUtil, neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, cloudStorageUtil: CloudStorageUtil): Unit = {
		val filteredMetadata = event.metadata.filter(x => !config.content_props_to_removed.contains(x._1))
		val createMetadata = filteredMetadata.filter(x => config.content_create_props.contains(x._1))
		val updateMetadata = filteredMetadata.filter(x => !config.content_create_props.contains(x._1))

		val originId = event.reqOriginData.getOrDefault("identifier","")
		var internalId, contentStage: String = ""
		var isCreated = false
		
		if (event.reqOriginData.nonEmpty && originId.nonEmpty) {
			val contentMetadata = neo4JUtil.getNodeProperties(event.identifier)
			if (!contentMetadata.isEmpty) {
				internalId = originId
				contentStage = "na"
			}
		}

		if(contentStage.isEmpty) {
			val contentMetadata = searchContent(event.identifier, config, httpUtil)
			if(contentMetadata.isEmpty) contentStage = "create" else {
				internalId = contentMetadata("contentId").asInstanceOf[String]
				contentStage = getContentStage(event.identifier, event.pkgVersion, contentMetadata)
			}
		}

		contentStage match {
			case "create" =>
				val result = createContent(event, createMetadata, config, httpUtil)
				internalId = result.get("identifier").asInstanceOf[String]
				if (StringUtils.isNotBlank(internalId)) {
					isCreated = true
					updateMetadata.put("versionKey", result.get("versionKey").asInstanceOf[Nothing])
				}
			case _ => logger.info("ContentUtil :: process :: Event Skipped for operations (create, upload, publish) for: " + event.identifier + " | Content Stage : " + contentStage)
		}

	}


	private def searchContent(identifier: String, config: ContentAutoCreatorConfig, httpUtil: HttpUtil): Map[String, AnyRef] = {
		val reqMap = new java.util.HashMap[String, AnyRef]() {
			put(ContentAutoCreatorConstants.REQUEST, new java.util.HashMap[String, AnyRef]() {
				put(ContentAutoCreatorConstants.FILTERS, new java.util.HashMap[String, AnyRef]() {
					put(ContentAutoCreatorConstants.CONTENT, "Content")
					put(ContentAutoCreatorConstants.STATUS, new util.ArrayList[String]())
					put(ContentAutoCreatorConstants.ORIGIN, identifier)
				})
				put(ContentAutoCreatorConstants.EXISTS, config.searchExistsFields.toArray[String])
				put(ContentAutoCreatorConstants.FIELDS, config.searchFields.toArray[String])
			})
		}

		val requestUrl = config.getString(ContentAutoCreatorConstants.SUNBIRD_CONTENT_SEARCH_URL,"")
		val httpResponse = httpUtil.post(requestUrl, JSONUtil.serialize(reqMap))
		if (httpResponse.status == 200) {
			val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
			val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val contents = result.getOrElse("content", List[Map[String,AnyRef]]()).asInstanceOf[List[Map[String,AnyRef]]]
			val count = result.getOrElse("count", 0).asInstanceOf[Int]
			if(count>0) {
				contents.filter(c => c.contains("originData")).filter(content => {
					val originDataStr = content.getOrElse("originData", "{}").asInstanceOf[String]
					val originData = JSONUtil.deserialize[Map[String, AnyRef]](originDataStr)
					val originId = originData.getOrElse("identifier", "").asInstanceOf[String]
					val repository = originData.getOrElse("repository", "").asInstanceOf[String]
					StringUtils.equalsIgnoreCase(originId, identifier) && repository.nonEmpty
				}).map(content => {
					Map("contentId" -> content.getOrDefault("identifier",""), "status" -> content.getOrDefault("status",""),
					"artifactUrl" -> content.getOrDefault("artifactUrl",""), "pkgVersion" -> content.getOrDefault("pkgVersion",0.0))
				}).head
			} else {
				logger.info("ContentAutoCreator :: searchContent :: Received 0 count while searching content for : " + identifier)
				Map.empty[String, AnyRef]
			}
		} else {
			throw new Exception("Content search failed for shallow copy check:" + identifier)
		}
	}

	private def getContentStage(identifier: String, pkgVersion: Double, metadata: Map[String, AnyRef]): String = {
		val status = metadata.get("status").asInstanceOf[String]
		val artifactUrl = metadata.get("artifactUrl").asInstanceOf[String]
		val pkgVer = metadata.getOrElse("pkgVersion", 0) match {
			case _: Integer => metadata.getOrElse("pkgVersion", 0).asInstanceOf[Integer].doubleValue()
			case _: Double => metadata.getOrElse("pkgVersion", 0).asInstanceOf[Double].doubleValue()
			case _ => metadata.getOrElse("pkgVersion", "0").toString.toDouble
		}

		if (!ContentAutoCreatorConstants.FINAL_STATUS.contains(status)) { if (artifactUrl.nonEmpty) "review" else "update" }
		else if (pkgVersion > pkgVer) "update"
		else {
			logger.info("ContentAutoCreator :: getContentStage :: Skipped Processing for : " + identifier + " | Internal Identifier : " + metadata.get("contentId") + " ,Status : " + status + " , artifactUrl : " + artifactUrl)
			"na"
		}
	}

	private def createContent(event: Event, createMetadata: Map[String,AnyRef], config: ContentAutoCreatorConfig, httpUtil: HttpUtil): Map[String, AnyRef] = {
		val updateMetadataFields = if(event.eData.getOrDefault(ContentAutoCreatorConstants.IDENTIFIER, "").asInstanceOf[String].nonEmpty) {
			createMetadata + (ContentAutoCreatorConstants.IDENTIFIER -> event.eData.getOrDefault(ContentAutoCreatorConstants.IDENTIFIER, "")) - ContentAutoCreatorConstants.CONTENT_TYPE
		} else {
			createMetadata + (ContentAutoCreatorConstants.IDENTIFIER -> event.identifier, ContentAutoCreatorConstants.ORIGIN -> event.identifier,
				ContentAutoCreatorConstants.ORIGIN_DATA -> Map[String,AnyRef](ContentAutoCreatorConstants.IDENTIFIER -> event.identifier,
					ContentAutoCreatorConstants.REPOSITORY -> event.repository)) - ContentAutoCreatorConstants.CONTENT_TYPE
		}
		logger.info("ContentAutoCreator :: createContent :: updateMetadataFields : " + updateMetadataFields)
		val reqMap = new java.util.HashMap[String, AnyRef]() {
			put(ContentAutoCreatorConstants.REQUEST, new java.util.HashMap[String, AnyRef]() {
				put(ContentAutoCreatorConstants.CONTENT, new java.util.HashMap[String, AnyRef]() {
					put(ContentAutoCreatorConstants.CONTENT, updateMetadataFields)
				})
			})
		}

		val headers = Map[String, String]("X-Channel-Id" -> event.channel, "Content-Type" -> ContentAutoCreatorConstants.APPLICATION_JSON)
		val requestUrl = config.getString(ContentAutoCreatorConstants.KP_CS_BASE_URL,"") + "/content/v4/create"
		val httpResponse = httpUtil.post(requestUrl, JSONUtil.serialize(reqMap), headers)
		if (httpResponse.status == 200) {
			val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
			val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val contentId = result.getOrElse(ContentAutoCreatorConstants.IDENTIFIER, "").asInstanceOf[String]
			logger.info("ContentAutoCreator :: createContent :: Content Created Successfully with identifier : " + contentId)
			result
		} else {
			logger.info("ContentAutoCreator :: createContent :: Invalid Response received while creating content for : " + event.identifier + getErrorDetails(httpResponse))
			throw new Exception("Invalid Response received while creating content for :" + event.identifier)
		}
	}

	private def getErrorDetails(httpResponse: HTTPResponse): String = {
		val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
		if (null != response) " | Response Code :" + httpResponse.status + " | Result : " + response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]] + " | Error Message : " + response.getOrElse("params", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
		else " | Null Response Received."
	}

}
