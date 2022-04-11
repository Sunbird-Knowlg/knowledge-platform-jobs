package org.sunbird.job.contentautocreator.helpers

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.contentautocreator.domain.Event
import org.sunbird.job.contentautocreator.task.ContentAutoCreatorConfig
import org.sunbird.job.contentautocreator.util.{ContentAutoCreatorConstants, GoogleDriveUtil}
import org.sunbird.job.exception.ServerException
import org.sunbird.job.util._

import java.io.File
import java.util
import scala.collection.convert.ImplicitConversions.`map AsJavaMap`

trait ContentAutoCreator extends ContentCollectionUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[ContentAutoCreator])

	def process(config: ContentAutoCreatorConfig, event: Event, httpUtil: HttpUtil, neo4JUtil: Neo4JUtil, cloudStorageUtil: CloudStorageUtil): Boolean = {
		val stage = event.eData.getOrDefault("stage","").asInstanceOf[String].trim
		val filteredMetadata = event.metadata.filter(x => !config.content_props_to_removed.contains(x._1))
		val createMetadata = filteredMetadata.filter(x => config.content_create_props.contains(x._1))
		val updateMetadata = filteredMetadata.filter(x => !config.content_create_props.contains(x._1))
		val delayUpload = if (StringUtils.equalsIgnoreCase(event.mimeType, "application/vnd.ekstep.h5p-archive")) 6 * config.apiCallDelay else config.apiCallDelay

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
				contentStage = getContentStage(event.identifier, event.pkgVersion, contentMetadata).trim
			}
		}
		logger.info("ContentAutoCreator :: process :: internalId: " + internalId + " || contentStage: " + contentStage + " || event.stage:: " + stage)
		var linkToCollection = false
		var isContentPublished = false

		try {
			contentStage match {
				case "create" =>
					internalId = createContent(event, createMetadata, config, httpUtil)
					updateContent(event.channel, internalId, updateMetadata, config, httpUtil, cloudStorageUtil)
					if (!stage.equalsIgnoreCase("create")) {
						uploadContent(event.channel, internalId, event.artifactUrl, event.mimeType, config, httpUtil, cloudStorageUtil)
						linkToCollection = true
						delay(delayUpload)
						if (!stage.equalsIgnoreCase("upload")) {
							reviewContent(event.channel, internalId, config, httpUtil)
							delay(config.apiCallDelay)
							if (!stage.equalsIgnoreCase("review")) {
								publishContent(event.channel, internalId, event.metadata("lastPublishedBy").asInstanceOf[String], config, httpUtil)
							}
							isContentPublished = true
						}
					}
				case "update" =>
					updateContent(event.channel, internalId, updateMetadata, config, httpUtil, cloudStorageUtil)
					if (!stage.equalsIgnoreCase("create")) {
						uploadContent(event.channel, internalId, event.artifactUrl, event.mimeType, config, httpUtil, cloudStorageUtil)
						linkToCollection = true
						delay(delayUpload)
						if (!stage.equalsIgnoreCase("upload")) {
							reviewContent(event.channel, internalId, config, httpUtil)
							delay(config.apiCallDelay)
							if (!stage.equalsIgnoreCase("review")) {
								publishContent(event.channel, internalId, event.metadata("lastPublishedBy").asInstanceOf[String], config, httpUtil)
							}
							isContentPublished = true
						}
					}
				case "upload" =>
					uploadContent(event.channel, internalId, event.artifactUrl, event.mimeType, config, httpUtil, cloudStorageUtil)
					linkToCollection = true
					delay(delayUpload)
					if (!stage.equalsIgnoreCase("upload")) {
						reviewContent(event.channel, internalId, config, httpUtil)
						delay(config.apiCallDelay)
						if (!stage.equalsIgnoreCase("review")) {
							publishContent(event.channel, internalId, event.metadata("lastPublishedBy").asInstanceOf[String], config, httpUtil)
						}
						isContentPublished = true
					}
				case "review" =>
					linkToCollection = true
					reviewContent(event.channel, internalId, config, httpUtil)
					delay(config.apiCallDelay)
					if (!stage.equalsIgnoreCase("review")) {
						publishContent(event.channel, internalId, event.metadata("lastPublishedBy").asInstanceOf[String], config, httpUtil)
					}
					isContentPublished = true
				case "publish" =>
					linkToCollection = true
					publishContent(event.channel, internalId, event.metadata("lastPublishedBy").asInstanceOf[String], config, httpUtil)
					isContentPublished = true
				case _ => logger.info("ContentAutoCreator :: process :: Event Skipped for operations (create, upload, publish) for: " + event.identifier + " | Content Stage : " + contentStage)
			}

			if(event.collection.nonEmpty && (linkToCollection || contentStage.equalsIgnoreCase("na"))) {
				linkCollection(internalId, event.collection)(config, httpUtil)
			} else logger.info("ContentAutoCreator :: process :: Textbook Linking Skipped because received empty collection/textbookInfo for : " + event.identifier)

			logger.info("ContentAutoCreator :: process :: finished processing for: " + event.identifier)
			isContentPublished
		} catch {
			case e: Exception => e.printStackTrace()
				if(internalId.nonEmpty) updateStatus(event.channel, internalId, e.getMessage, config, httpUtil)
				throw e
		}

	}

	private def searchContent(identifier: String, config: ContentAutoCreatorConfig, httpUtil: HttpUtil): Map[String, AnyRef] = {
		val reqMap = new java.util.HashMap[String, AnyRef]() {
			put(ContentAutoCreatorConstants.REQUEST, new java.util.HashMap[String, AnyRef]() {
				put(ContentAutoCreatorConstants.FILTERS, new java.util.HashMap[String, AnyRef]() {
					put(ContentAutoCreatorConstants.OBJECT_TYPE, "Content")
					put(ContentAutoCreatorConstants.STATUS, new util.ArrayList[String]())
					put(ContentAutoCreatorConstants.ORIGIN, identifier)
				})
				put(ContentAutoCreatorConstants.EXISTS, config.searchExistsFields.toArray[String])
				put(ContentAutoCreatorConstants.FIELDS, config.searchFields.toArray[String])
			})
		}

		val requestUrl = s"${config.searchServiceBaseUrl}/v3/search"
		logger.info("ContentAutoCreator :: searchContent :: Search Content requestUrl: " + requestUrl)
		logger.info("ContentAutoCreator :: searchContent :: Search Content reqMap: " + reqMap)
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
		val status = metadata("status").asInstanceOf[String]
		val artifactUrl = metadata("artifactUrl").asInstanceOf[String]
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
		val requestUrl = s"${config.contentServiceBaseUrl}/content/v4/read/$identifier?mode=edit"
		logger.info("ContentAutoCreator :: read :: Reading content having identifier : " + identifier)
		val headers = Map[String, String](ContentAutoCreatorConstants.X_CHANNEL_ID -> channelId, ContentAutoCreatorConstants.CONTENT_TYPE_HEADER -> ContentAutoCreatorConstants.APPLICATION_JSON)
		logger.info("ContentAutoCreator :: read :: Reading content - requestUrl : " + requestUrl)
		val httpResponse = httpUtil.get(requestUrl, headers)
		if (httpResponse.status == 200) {
			val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
			val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val content = result.getOrElse("content", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val contentId = content.getOrDefault("identifier", "").asInstanceOf[String].replace(".img", "")
			if (StringUtils.equalsIgnoreCase(identifier, contentId)) logger.info("ContentAutoCreator :: read :: Content Fetched Successfully with identifier : " + contentId)
			else throw new ServerException("SYSTEM_ERROR", "Invalid Response received while reading content for : " + identifier)
			content
		}
		else {
			val errMsg = getErrorDetails(httpResponse)
			logger.info("ContentAutoCreator :: read :: Invalid Response received while reading content for : " + identifier + errMsg)
			throw new ServerException("SYSTEM_ERROR", "Invalid Response received while reading content for : " + identifier + errMsg)
		}
	}

	private def createContent(event: Event, createMetadata: Map[String,AnyRef], config: ContentAutoCreatorConfig, httpUtil: HttpUtil): String = {
		val createMetadataFields = if(event.eData.getOrDefault(ContentAutoCreatorConstants.IDENTIFIER, "").asInstanceOf[String].nonEmpty) {
			createMetadata + (ContentAutoCreatorConstants.IDENTIFIER -> event.eData.getOrDefault(ContentAutoCreatorConstants.IDENTIFIER, "")) - ContentAutoCreatorConstants.CONTENT_TYPE
		} else {
			createMetadata + (ContentAutoCreatorConstants.IDENTIFIER -> event.identifier, ContentAutoCreatorConstants.ORIGIN -> event.identifier,
				ContentAutoCreatorConstants.ORIGIN_DATA -> Map[String,AnyRef](ContentAutoCreatorConstants.IDENTIFIER -> event.identifier,
					ContentAutoCreatorConstants.REPOSITORY -> event.repository)) - ContentAutoCreatorConstants.CONTENT_TYPE
		}
		logger.info("ContentAutoCreator :: createContent :: createMetadataFields : " + createMetadataFields)
		val reqMap = new java.util.HashMap[String, AnyRef]() {
			put(ContentAutoCreatorConstants.REQUEST, new java.util.HashMap[String, AnyRef]() {
					put(ContentAutoCreatorConstants.CONTENT, createMetadataFields)
			})
		}

		val headers = Map[String, String](ContentAutoCreatorConstants.X_CHANNEL_ID -> event.channel, ContentAutoCreatorConstants.CONTENT_TYPE_HEADER -> ContentAutoCreatorConstants.APPLICATION_JSON)
		val requestUrl = s"${config.contentServiceBaseUrl}/content/v4/create"
		logger.info("ContentAutoCreator :: createContent :: Create Content requestUrl: " + requestUrl)
		logger.info("ContentAutoCreator :: createContent :: Create Content reqMap: " + reqMap)
		val httpResponse = httpUtil.post(requestUrl, JSONUtil.serialize(reqMap), headers)
		if (httpResponse.status == 200) {
			val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
			val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val contentId = result.getOrElse(ContentAutoCreatorConstants.IDENTIFIER, "").asInstanceOf[String]
			logger.info("ContentAutoCreator :: createContent :: Content Created Successfully with identifier : " + contentId)
			contentId
		} else {
			val errMsg = getErrorDetails(httpResponse)
			logger.info("ContentAutoCreator :: createContent :: Invalid Response received while creating content for : " + event.identifier + errMsg)
			if (errMsg.contains("Object already exists with identifier")) event.identifier else throw new ServerException("SYSTEM_ERROR", "Invalid Response received while creating content for :" + event.identifier + errMsg)
		}
	}

	private def updateContent(channel: String, internalId: String, updateMetadata: Map[String,AnyRef], config: ContentAutoCreatorConfig, httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): Unit = {
		val readMetadata = read(channel, internalId, config, httpUtil)

		val appIconUrl = updateMetadata.getOrDefault("appIcon", "").asInstanceOf[String].trim
		val updatedAppIcon = if (appIconUrl != null && appIconUrl.nonEmpty) {
			logger.info("ContentAutoCreator :: update :: Initiating Icon download for : " + internalId + " | appIconUrl : " + appIconUrl)
			val file = getFile(internalId, appIconUrl, "image", config, httpUtil)
			logger.info("ContentAutoCreator :: update :: Icon downloaded for : " + internalId + " | appIconUrl : " + appIconUrl)
			if (null == file || !file.exists) throw new Exception("Error Occurred while downloading appIcon file for " + internalId + " | File Url : " + appIconUrl)
			val urls = uploadArtifact(file, internalId, config, cloudStorageUtil)
			val updatedAppIcon = if (null != urls && StringUtils.isNotBlank(urls(1))) {
				val appIconBlobUrl = urls(1)
				logger.info("ContentAutoCreator :: update :: Icon Uploaded Successfully to cloud for : " + internalId + " | appIconUrl : " + appIconUrl + " | appIconBlobUrl : " + appIconBlobUrl)
				appIconBlobUrl
			}
			FileUtils.deleteQuietly(file)
			updatedAppIcon
		} else ""

		val updateMetadataFields = updateMetadata + (ContentAutoCreatorConstants.VERSION_KEY -> readMetadata(ContentAutoCreatorConstants.VERSION_KEY).asInstanceOf[String], ContentAutoCreatorConstants.APP_ICON -> updatedAppIcon)

		logger.info("ContentAutoCreator :: updateContent :: updateMetadataFields : " + updateMetadataFields)
		val reqMap = new java.util.HashMap[String, AnyRef]() {
			put(ContentAutoCreatorConstants.REQUEST, new java.util.HashMap[String, AnyRef]() {
					put(ContentAutoCreatorConstants.CONTENT, updateMetadataFields)
			})
		}

		val headers = Map[String, String](ContentAutoCreatorConstants.X_CHANNEL_ID -> channel, ContentAutoCreatorConstants.CONTENT_TYPE_HEADER -> ContentAutoCreatorConstants.APPLICATION_JSON)
		val requestUrl = s"${config.contentServiceBaseUrl}/content/v4/update/$internalId"
		logger.info("ContentAutoCreator :: updateContent :: Update Content requestUrl: " + requestUrl)
		logger.info("ContentAutoCreator :: updateContent :: Update Content reqMap: " + reqMap)
		val httpResponse = httpUtil.patch(requestUrl, JSONUtil.serialize(reqMap), headers)
		if (httpResponse.status == 200) {
			val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
			val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val contentId = result.getOrElse(ContentAutoCreatorConstants.IDENTIFIER, "").asInstanceOf[String]
			logger.info("ContentAutoCreator :: updateContent :: Content Updated Successfully with identifier : " + contentId)
		} else {
			logger.info("ContentAutoCreator :: updateContent :: Invalid Response received while updating content for : " + internalId + getErrorDetails(httpResponse))
			throw new ServerException("SYSTEM_ERROR", "Invalid Response received while updating content for :" + internalId)
		}
	}

	private def uploadContent(channel: String, internalId: String, sourceUrl: String, mimeType: String, config: ContentAutoCreatorConfig, httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): Unit = {
		val allowedArtifactSources: List[String] = config.artifactAllowedSources

		if(allowedArtifactSources.nonEmpty && !allowedArtifactSources.exists(x => sourceUrl.contains(x)))
			throw new ServerException("SYSTEM_ERROR", "Artifact Source is not from allowed one for : " + internalId + " | artifactUrl: " + sourceUrl + " | Allowed Sources : " + allowedArtifactSources)

		if (sourceUrl != null && sourceUrl.nonEmpty) {
			logger.info("ContentAutoCreator :: uploadContent :: Initiating sourceFile download for : " + internalId + " | sourceUrl : " + sourceUrl)
			val file: File = getFile(internalId, sourceUrl, mimeType, config, httpUtil)
			logger.info("ContentAutoCreator :: uploadContent :: sourceFile downloaded for : " + internalId + " | sourceUrl : " + sourceUrl)
			if (null == file || !file.exists) throw new Exception("Error Occurred while downloading sourceUrl file for " + internalId + " | File Url : " + sourceUrl)
			logger.info("ContentAutoCreator :: uploadContent :: File Path for " + internalId + " is : " + file.getAbsolutePath + " | File Size : " + file.length)
			if (file.length > config.artifactMaxSize && !config.bulkUploadMimeTypes.contains(mimeType)) {
				logger.info("ContentAutoCreator :: uploadContent :: File Size is larger than allowed file size allowed in upload api for : " + internalId + " | File Size (MB): " + (file.length / 1048576) + " | mimeType : " + mimeType)
				throw new ServerException("SYSTEM_ERROR", "File Size is larger than allowed file size allowed in upload api for : " + internalId + " | File Size (MB): " + (file.length / 1048576) + " | mimeType : " + mimeType)
			}

			val urls = uploadArtifact(file, internalId, config, cloudStorageUtil)
			FileUtils.deleteQuietly(file.getParentFile)
			if (null != urls && StringUtils.isNotBlank(urls(1))) {
				val sourceFileBlobUrl = urls(1)
				logger.info("ContentAutoCreator :: uploadContent :: sourceUrl Uploaded Successfully to cloud for : " + internalId + " | sourceUrl : " + sourceUrl + " | sourceFileBlobUrl : " + sourceFileBlobUrl)

				val headers = Map[String, String](ContentAutoCreatorConstants.X_CHANNEL_ID -> channel)
				val requestUrl = s"${config.contentServiceBaseUrl}/content/v4/upload/$internalId"
				logger.info("ContentAutoCreator :: uploadContent :: Upload Content requestUrl: " + requestUrl)
				logger.info("ContentAutoCreator :: uploadContent :: Upload Content sourceFileBlobUrl: " + sourceFileBlobUrl)
				val httpResponse = httpUtil.postFilePath(requestUrl, "fileUrl", sourceFileBlobUrl, headers)
				if (httpResponse.status == 200) {
					val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
					val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
					val artifactUrl = result.getOrElse("artifactUrl", "").asInstanceOf[String]
					logger.info("ContentAutoCreator :: uploadContent :: Content Uploaded Successfully for identifier : " + internalId + " | artifactUrl: " + artifactUrl)
				} else {
					logger.info("ContentAutoCreator :: uploadContent :: Invalid Response received while uploading source file for content : " + internalId + getErrorDetails(httpResponse))
					throw new ServerException("SYSTEM_ERROR", "Invalid Response received while uploading source file for content :" + internalId)
				}
			}	else throw new ServerException("SYSTEM_ERROR", "Artifact source file upload to cloud storage failed for :" + internalId)
		} else throw new ServerException("SYSTEM_ERROR", "Artifact Source is not available for content : " + internalId)
	}

	private def reviewContent(channel: String, identifier: String, config: ContentAutoCreatorConfig, httpUtil: HttpUtil): Unit = {
		val requestUrl = s"${config.contentServiceBaseUrl}/content/v4/review/$identifier"
		val reqMap = new java.util.HashMap[String, AnyRef]() {
			put(ContentAutoCreatorConstants.REQUEST, new java.util.HashMap[String, AnyRef]() {
				put(ContentAutoCreatorConstants.CONTENT, new java.util.HashMap[String, AnyRef]())
			})
		}
		val headers = Map[String, String](ContentAutoCreatorConstants.X_CHANNEL_ID -> channel, ContentAutoCreatorConstants.CONTENT_TYPE_HEADER -> ContentAutoCreatorConstants.APPLICATION_JSON)
		logger.info("ContentAutoCreator :: reviewContent :: Review Content requestUrl: " + requestUrl)
		logger.info("ContentAutoCreator :: reviewContent :: Review Content reqMap: " + reqMap)
		val httpResponse = httpUtil.post(requestUrl, JSONUtil.serialize(reqMap), headers)
		if (httpResponse.status == 200) {
			val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
			val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val contentId = result.getOrElse("node_id", "").asInstanceOf[String]
			logger.info("ContentAutoCreator :: reviewContent :: Content sent for Review Successfully with identifier : " + contentId)
		} else {
			logger.info("ContentAutoCreator :: reviewContent :: Invalid Response received while sending content for review for : " + identifier + getErrorDetails(httpResponse))
			throw new ServerException("SYSTEM_ERROR", "Invalid Response received while sending content for review for :" + identifier)
		}
	}

	private def publishContent(channel: String, identifier: String, lastPublishedBy: String, config: ContentAutoCreatorConfig, httpUtil: HttpUtil): Unit = {
		val requestUrl = s"${config.learningServiceBaseUrl}/content/v3/publish/" + identifier
		val reqMap = new java.util.HashMap[String, AnyRef]() {
			put(ContentAutoCreatorConstants.REQUEST, new java.util.HashMap[String, AnyRef]() {
				put(ContentAutoCreatorConstants.CONTENT, new java.util.HashMap[String, AnyRef]() {
					put("lastPublishedBy", lastPublishedBy)
				})
			})
		}
		val headers = Map[String, String](ContentAutoCreatorConstants.X_CHANNEL_ID -> channel, ContentAutoCreatorConstants.CONTENT_TYPE_HEADER -> ContentAutoCreatorConstants.APPLICATION_JSON)
		logger.info("ContentAutoCreator :: publishContent :: Publish Content requestUrl: " + requestUrl)
		logger.info("ContentAutoCreator :: publishContent :: Publish Content reqMap: " + reqMap)
		val httpResponse = httpUtil.post(requestUrl, JSONUtil.serialize(reqMap), headers)
		if (httpResponse.status == 200) {
			val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
			val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val publishStatus = result.getOrElse("publishStatus", "").asInstanceOf[String]
			if(publishStatus !=null && publishStatus.nonEmpty)
				logger.info("ContentAutoCreator :: publishContent :: Content sent for Publishing Successfully with identifier : " + identifier)
			else
				throw new ServerException("SYSTEM_ERROR", "Content Publish Call Failed For : " + identifier)
		} else {
			logger.info("ContentAutoCreator :: publishContent :: Invalid Response received while publishing content for : " + identifier + getErrorDetails(httpResponse))
			throw new ServerException("SYSTEM_ERROR", "Invalid Response received while publishing content for :" + identifier)
		}
	}

	private def updateStatus(channel: String, identifier: String, message: String, config: ContentAutoCreatorConfig, httpUtil: HttpUtil): Unit = {
		val errorMsg = if (message.nonEmpty) message	else "Processing Error"
		val requestUrl = s"${config.contentServiceBaseUrl}/content/v4/system/update/$identifier"
		val reqMap = new java.util.HashMap[String, AnyRef]() {
			put(ContentAutoCreatorConstants.REQUEST, new java.util.HashMap[String, AnyRef]() {
				put(ContentAutoCreatorConstants.CONTENT, new java.util.HashMap[String, AnyRef]() {
					put("status", "Failed")
					put("importError", errorMsg)
				})
			})
		}
		val headers = Map[String, String](ContentAutoCreatorConstants.X_CHANNEL_ID -> channel, ContentAutoCreatorConstants.CONTENT_TYPE_HEADER -> ContentAutoCreatorConstants.APPLICATION_JSON)
		val httpResponse = httpUtil.patch(requestUrl, JSONUtil.serialize(reqMap), headers)
		if (httpResponse.status == 200) {
			val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
			val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val nodeId = result.getOrElse("identifier", "").asInstanceOf[String]
			if(nodeId !=null && nodeId.nonEmpty)
				logger.info("ContentAutoCreator :: updateStatus :: Content failed status successfully updated for identifier : " + identifier)
			else
				throw new ServerException("SYSTEM_ERROR", "Content update status Call Failed For : " + identifier)
		} else {
			logger.info("ContentAutoCreator :: updateStatus :: Invalid Response received while updating failed status for : " + identifier + getErrorDetails(httpResponse))
			throw new ServerException("SYSTEM_ERROR", "Invalid Response received while updating failed status for : " + identifier)
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
			}
			file
		} catch {
			case e: ServerException =>
					logger.info("Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl + "Exception is : " + e.getMessage)
					throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl)
		}
	}

	private def getErrorDetails(httpResponse: HTTPResponse): String = {
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
			folder = folder + "/" + Slug.makeSlug(identifier, isTransliterate = true) + "/" + config.artifactFolder
			cloudStorageUtil.uploadFile(folder, uploadedFile, Option(true))
		} catch {
			case e: Exception => e.printStackTrace()
				logger.info("ContentAutoCreator :: uploadArtifact ::  Exception occurred while uploading artifact for : " + identifier + "Exception is : " + e.getMessage)
				throw new ServerException("ERR_CONTENT_UPLOAD_FILE", "Error while uploading the File.", e)
		}
	}

	private def delay(time: Long): Unit = {
		try Thread.sleep(time * 1000)
		catch { case e: Exception => }
	}

}
