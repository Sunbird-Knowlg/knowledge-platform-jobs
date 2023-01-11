package org.sunbird.job.cspmigrator.helpers

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.exception.ServerException
import org.sunbird.job.util.{CloudStorageUtil, HttpUtil, Slug}

import java.io.File
import scala.collection.JavaConverters._

trait CSPNeo4jMigrator extends MigrationObjectReader with MigrationObjectUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[CSPNeo4jMigrator])
	val streamableMimeTypes=List("video/mp4", "video/webm")

	def process(objMetadata: Map[String, AnyRef], config: CSPMigratorConfig, httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): Map[String, AnyRef] = {

		// fetch the objectType.
		// check if the object has only Draft OR only Live OR live/image if the objectType is content/collection.
		// fetch the string replace data from config
		// fetch the list of attributes for string replace from config
		// check the string of each property and perform string replace.
		// commit updated data to DB.
		// if objectType is Asset and mimeType is video, trigger streamingUrl generation
		// if objectType is content and mimeType is ECML, need to update ECML content body
		// if objectType is collection, fetch hierarchy data and update cassandra data with the replaced string
		// if objectType is content/collection and the node is Live, trigger the LiveNodePublisher flink job
		// if objectType is AssessmentItem, migrate cassandra data as well
		// update the migrationVersion of the object

		// validation of the replace URL paths to be done. If not available, Fail migration
		// For Migration Failed contents set migrationVersion to 0.1
		// For collection, verify if all childNodes are having live migrated contents - REQUIRED for Draft/Image version?

		val objectType: String = objMetadata.getOrElse("objectType","").asInstanceOf[String]
		val mimeType: String = objMetadata.getOrElse("mimeType","").asInstanceOf[String]
		val identifier: String = objMetadata.getOrElse("identifier", "").asInstanceOf[String]
		val fieldsToMigrate: List[String] = if (config.getConfig.hasPath("neo4j_fields_to_migrate."+objectType.toLowerCase())) config.getConfig.getStringList("neo4j_fields_to_migrate."+objectType.toLowerCase()).asScala.toList
													else throw new ServerException("ERR_CONFIG_NOT_FOUND", "Fields to migrate configuration not found for objectType: " + objectType)

		logger.info(s"""CSPNeo4jMigrator:: process:: starting neo4j fields migration for $identifier - $objectType fields:: $fieldsToMigrate""")
		val migratedMetadataFields: Map[String, String] =  fieldsToMigrate.flatMap(migrateField => {
			if(objMetadata.contains(migrateField) && (!migrateField.equalsIgnoreCase("streamingUrl") || !streamableMimeTypes.contains(mimeType) )) {
				val metadataFieldValue = objMetadata.getOrElse(migrateField, "").asInstanceOf[String]

				// TODO: call a method to validate the url, upload to cloud set the url to migrated value
				val tempMetadataFieldValue = handleGoogleDriveMetadata(metadataFieldValue,identifier, mimeType, config, httpUtil, cloudStorageUtil)

				//val migrateValue: String = StringUtils.replaceEach(metadataFieldValue, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
				//if(config.copyMissingFiles) verifyFile(identifier, metadataFieldValue, migrateValue, migrateField, config)(httpUtil, cloudStorageUtil)

				val migrateValue: String = if(StringUtils.isNotBlank(tempMetadataFieldValue))
					StringUtils.replaceEach(tempMetadataFieldValue, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
					else	null
				if(config.copyMissingFiles && StringUtils.isNotBlank(migrateValue)) verifyFile(identifier, tempMetadataFieldValue, migrateValue, migrateField, config)(httpUtil, cloudStorageUtil)
				Map(migrateField -> migrateValue)
			} else Map.empty[String, String]
		}).filter(record => record._1.nonEmpty).toMap[String, String]

		logger.info(s"""CSPNeo4jMigrator:: process:: $identifier - $objectType migratedMetadataFields:: $migratedMetadataFields""")
		migratedMetadataFields
	}

	/*def handleGoogleDriveMetadata(fileUrl: String, contentId: String, mimeType: String, config: CSPMigratorConfig, httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): String = {
		if (StringUtils.isNotBlank(fileUrl) && fileUrl.contains("drive.google.com")) {
			val file = getFile(contentId, fileUrl, "image", config, httpUtil)
			logger.info("ContentAutoCreator :: update :: Icon downloaded for : " + contentId + " | appIconUrl : " + fileUrl)

			if (null == file || !file.exists){
				logger.info("Error Occurred while downloading appIcon file for " + contentId + " | File Url : " + fileUrl)
				null
			} else{
				val urls = uploadArtifact(file, contentId, config, cloudStorageUtil)
				val url = if (null != urls && StringUtils.isNotBlank(urls(1))) {
					val  blobUrl = urls(1)
					logger.info("CSPNeo4jMigrator :: handleGoogleDriveMetadata :: Icon Uploaded Successfully to cloud for : " + contentId + " | appIconUrl : " + fileUrl + " | appIconBlobUrl : " + blobUrl)
					FileUtils.deleteQuietly(file)
					blobUrl
				}else{
					null
				}
				url
			}
		}else{
			fileUrl
		}
	}

	private def uploadArtifact(uploadedFile: File, identifier: String, config: CSPMigratorConfig, cloudStorageUtil: CloudStorageUtil) = {
		try {
			var folder = config.contentFolder
			folder = folder + "/" + Slug.makeSlug(identifier, isTransliterate = true) + "/" + config.artifactFolder
			cloudStorageUtil.uploadFile(folder, uploadedFile, Option(true))
		} catch {
			case e: Exception => e.printStackTrace()
				logger.info("ContentAutoCreator :: uploadArtifact ::  Exception occurred while uploading artifact for : " + identifier + "Exception is : " + e.getMessage)
				throw new ServerException("ERR_CONTENT_UPLOAD_FILE", "Error while uploading the File.", e)
		}
	}*/

	/*@throws[Exception]
	private def getFile(identifier: String, fileUrl: String, mimeType: String, config: CSPMigratorConfig, httpUtil: HttpUtil): File = {
		try {
			val fileId = fileUrl.split("download&id=")(1)
			if (StringUtils.isBlank(fileId)) {
				//throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl)
				logger.info("Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl)
				null
			}else{
				GoogleDriveUtil.downloadFile(fileId, getBasePath(identifier, config), mimeType)(config)
			}
		} catch {
			case e: ServerException =>
				logger.info("Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl + "Exception is : " + e.getMessage)
				throw e
			case ex: Exception =>
				logger.info("Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl + "Exception is : " + ex.getMessage)
				throw new ServerException("ERR_INVALID_UPLOAD_FILE_URL", "Invalid fileUrl received for : " + identifier + " | fileUrl : " + fileUrl)
		}
	}

	private def getBasePath(objectId: String, config: CSPMigratorConfig): String = {
		if (StringUtils.isNotBlank(objectId)) config.temp_file_location + File.separator + objectId + File.separator + "_temp_" + System.currentTimeMillis
		else config.temp_file_location + File.separator + "_temp_" + System.currentTimeMillis
	}*/

}
