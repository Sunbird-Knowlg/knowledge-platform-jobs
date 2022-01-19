package org.sunbird.job.contentautocreator.helpers

import kong.unirest.Unirest
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.contentautocreator.domain.Event
import org.sunbird.job.contentautocreator.model.{ExtDataConfig, ObjectData}
import org.sunbird.job.contentautocreator.util.{ContentAutoCreatorConstants, GoogleDriveUtil}
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.exception.ServerException
import org.sunbird.job.task.ContentAutoCreatorConfig
import org.sunbird.job.util._

import java.io.File
import java.util
import scala.collection.convert.ImplicitConversions.`map AsJavaMap`
import scala.collection.mutable

trait ContentAutoCreator extends ObjectUpdater with ContentCollectionUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[ContentAutoCreator])

	def process(config: ContentAutoCreatorConfig, event: Event, httpUtil: HttpUtil, neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, cloudStorageUtil: CloudStorageUtil): Unit = {
		val stage = event.eData.getOrDefault("stage","").asInstanceOf[String]
		val filteredMetadata = event.metadata.filter(x => !config.content_props_to_removed.contains(x._1))
		val createMetadata = filteredMetadata.filter(x => config.content_create_props.contains(x._1))
		val updateMetadata = filteredMetadata.filter(x => !config.content_create_props.contains(x._1))

		val originId = event.reqOriginData.getOrDefault("identifier","")
		var internalId, contentStage: String = ""
		
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
				createContent(event, createMetadata, config, httpUtil)
				updateContent(event, internalId, updateMetadata, config, httpUtil, cloudStorageUtil)
			case "update" => updateContent(event, internalId, updateMetadata, config, httpUtil, cloudStorageUtil)


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
					"artifactUrl" -> content.getOrDefault("artifactUrl",""), "pkgVersion" -> content.getOrDefault("pkgVersion",0.0.asInstanceOf[AnyRef]))
				}).head
			} else {
				logger.info("ContentAutoCreator :: searchContent :: Received 0 count while searching content for : " + identifier)
				Map.empty[String, AnyRef]
			}
		} else {
			throw new ServerException("ERR_API_CALL", "Invalid Response received while searching content for : " + identifier + getErrorDetails(httpResponse))
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

	private def read(channelId: String, identifier: String, config: ContentAutoCreatorConfig, httpUtil: HttpUtil) = {
		val requestUrl = config.getString(ContentAutoCreatorConstants.KP_CS_BASE_URL,"") + "/content/v4/read/" + identifier + "mode=edit"
		logger.info("ContentUtil :: read :: Reading content having identifier : " + identifier)
		val headers = Map[String, String]("X-Channel-Id" -> channelId, "Content-Type" -> ContentAutoCreatorConstants.APPLICATION_JSON)
		val httpResponse = httpUtil.get(requestUrl, headers)
		if (httpResponse.status == 200) {
			val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
			val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val content = result.getOrElse("content", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val contentId = content.getOrDefault("identifier", "").asInstanceOf[String].replace(".img", "")
			if (StringUtils.equalsIgnoreCase(identifier, contentId)) logger.info("ContentUtil :: read :: Content Fetched Successfully with identifier : " + contentId)
			else throw new ServerException("SYSTEM_ERROR", "Invalid Response received while reading content for : " + identifier)
			content
		}
		else {
			logger.info("ContentUtil :: read :: Invalid Response received while reading content for : " + identifier + getErrorDetails(httpResponse))
			throw new ServerException("SYSTEM_ERROR", "Invalid Response received while reading content for : " + identifier + getErrorDetails(httpResponse))
		}
	}


	private def createContent(event: Event, createMetadata: Map[String,AnyRef], config: ContentAutoCreatorConfig, httpUtil: HttpUtil): Unit = {
		val createMetadataFields = if(event.eData.getOrDefault(ContentAutoCreatorConstants.IDENTIFIER, "").asInstanceOf[String].nonEmpty) {
			createMetadata + (ContentAutoCreatorConstants.IDENTIFIER -> event.eData.getOrDefault(ContentAutoCreatorConstants.IDENTIFIER, "")) - ContentAutoCreatorConstants.CONTENT_TYPE
		} else {
			createMetadata + (ContentAutoCreatorConstants.IDENTIFIER -> event.identifier, ContentAutoCreatorConstants.ORIGIN -> event.identifier,
				ContentAutoCreatorConstants.ORIGIN_DATA -> Map[String,AnyRef](ContentAutoCreatorConstants.IDENTIFIER -> event.identifier,
					ContentAutoCreatorConstants.REPOSITORY -> event.repository)) - ContentAutoCreatorConstants.CONTENT_TYPE
		}
		logger.info("ContentAutoCreator :: createContent :: updateMetadataFields : " + createMetadataFields)
		val reqMap = new java.util.HashMap[String, AnyRef]() {
			put(ContentAutoCreatorConstants.REQUEST, new java.util.HashMap[String, AnyRef]() {
					put(ContentAutoCreatorConstants.CONTENT, createMetadataFields)
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
		} else {
			logger.info("ContentAutoCreator :: createContent :: Invalid Response received while creating content for : " + event.identifier + getErrorDetails(httpResponse))
			throw new ServerException("SYSTEM_ERROR", "Invalid Response received while creating content for :" + event.identifier)
		}
	}

	private def updateContent(event: Event, internalId: String, updateMetadata: Map[String,AnyRef], config: ContentAutoCreatorConfig, httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): Unit = {
		val readMetadata = read(event.channel, internalId, config, httpUtil)
		val updateMetadataFields = updateMetadata + (ContentAutoCreatorConstants.VERSION_KEY -> readMetadata.get(ContentAutoCreatorConstants.VERSION_KEY).asInstanceOf[String])

		val appIconUrl = updateMetadata.getOrDefault("appIcon", "").asInstanceOf[String].trim
		if (appIconUrl != null && appIconUrl.nonEmpty) {
			logger.info("ContentUtil :: update :: Initiating Icon download for : " + internalId + " | appIconUrl : " + appIconUrl)
			val file = getFile(internalId, appIconUrl, "image", config, httpUtil)
			logger.info("ContentUtil :: update :: Icon downloaded for : " + internalId + " | appIconUrl : " + appIconUrl)
			if (null == file || !file.exists) throw new Exception("Error Occurred while downloading appIcon file for " + internalId + " | File Url : " + appIconUrl)
			val urls = uploadArtifact(file, internalId, config, cloudStorageUtil)
			if (null != urls && StringUtils.isNotBlank(urls(1))) {
				val appIconBlobUrl = urls(1)
				logger.info("ContentUtil :: update :: Icon Uploaded Successfully to cloud for : " + internalId + " | appIconUrl : " + appIconUrl + " | appIconBlobUrl : " + appIconBlobUrl)
				updateMetadata.put("appIcon", appIconBlobUrl)
			}
		}

		logger.info("ContentAutoCreator :: updateContent :: updateMetadataFields : " + updateMetadataFields)
		val reqMap = new java.util.HashMap[String, AnyRef]() {
			put(ContentAutoCreatorConstants.REQUEST, new java.util.HashMap[String, AnyRef]() {
					put(ContentAutoCreatorConstants.CONTENT, updateMetadataFields)
			})
		}

		val headers = Map[String, String]("X-Channel-Id" -> event.channel, "Content-Type" -> ContentAutoCreatorConstants.APPLICATION_JSON)
		val requestUrl = config.getString(ContentAutoCreatorConstants.KP_CS_BASE_URL,"") + "/content/v4/update" + internalId
		val httpResponse = httpUtil.patch(requestUrl, JSONUtil.serialize(reqMap), headers)
		if (httpResponse.status == 200) {
			val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
			val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val contentId = result.getOrElse(ContentAutoCreatorConstants.IDENTIFIER, "").asInstanceOf[String]
			logger.info("ContentAutoCreator :: updateContent :: Content Updated Successfully with identifier : " + contentId)
		} else {
			logger.info("ContentAutoCreator :: updateContent :: Invalid Response received while updating content for : " + event.identifier + getErrorDetails(httpResponse))
			throw new ServerException("SYSTEM_ERROR", "Invalid Response received while creating content for :" + event.identifier)
		}
	}

	@throws[Exception]
	private def getFile(identifier: String, fileUrl: String, mimeType: String, config: ContentAutoCreatorConfig, httpUtil: HttpUtil): File = {

		try {
			val file: File = if (StringUtils.isNotBlank(fileUrl) && fileUrl.contains("drive.google.com")) {
				val fileId = fileUrl.split("download&id=")(1)
				if (StringUtils.isBlank(fileId)) throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl)
				GoogleDriveUtil.downloadFile(fileId, getBasePath(identifier, config), mimeType)(config)
			} else {
				val filePath = getBasePath(identifier, config)
				httpUtil.downloadFile(fileUrl, filePath)
				val downloadedFile = new File(filePath)
				if(downloadedFile.exists()) downloadedFile else throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl)
			}
			file
		} catch {
			case e: ServerException =>
				if (e.isInstanceOf[ServerException]) throw e
				else {
					logger.info("Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl + "Exception is : " + e.getMessage)
					throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl)
				}
		}
	}

	private def getErrorDetails(httpResponse: HTTPResponse) = {
		val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
		if (null != response) " | Response Code :" + httpResponse.status + " | Result : " + response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]] + " | Error Message : " + response.getOrElse("params", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
		else " | Null Response Received."
	}

	private def getBasePath(objectId: String, config: ContentAutoCreatorConfig): String = {
		if (StringUtils.isNotBlank(objectId)) config.temp_file_location + File.separator + objectId + File.separator + "_temp_" + System.currentTimeMillis
		else config.temp_file_location + File.separator + "_temp_" + System.currentTimeMillis
	}

	private def uploadArtifact(uploadedFile: File, identifier: String, config: ContentAutoCreatorConfig, cloudStorageUtil: CloudStorageUtil) = {
		try {
			var folder = config.contentFolder
			folder = folder + "/" + Slug.makeSlug(identifier, true) + "/" + config.artifactFolder
			cloudStorageUtil.uploadFile(folder, uploadedFile, Option(true))
		} catch {
			case e: Exception =>
				logger.info("ContentUtil :: uploadArtifact ::  Exception occurred while uploading artifact for : " + identifier + "Exception is : " + e.getMessage)
				e.printStackTrace
				throw new ServerException("ERR_CONTENT_UPLOAD_FILE", "Error while uploading the File.", e)
		}
	}
}
