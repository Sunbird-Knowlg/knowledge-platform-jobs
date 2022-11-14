package org.sunbird.job.cspmigrator.helpers

import com.datastax.driver.core.Row
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.exception.ServerException
import org.sunbird.job.util._

import java.io.{File, IOException}
import java.net.URL
import scala.collection.JavaConverters._

trait CSPCassandraMigrator extends MigrationObjectReader with MigrationObjectUpdater with URLExtractor {

	private[this] val logger = LoggerFactory.getLogger(classOf[CSPCassandraMigrator])

	def process(objMetadata: Map[String, AnyRef], status: String, config: CSPMigratorConfig, httpUtil: HttpUtil, cassandraUtil: CassandraUtil, cloudStorageUtil: CloudStorageUtil): Unit = {

		val objectType: String = objMetadata.getOrElse("objectType","").asInstanceOf[String]
		val mimeType: String = objMetadata.getOrElse("mimeType","").asInstanceOf[String]
		val identifier: String = objMetadata.getOrElse("identifier", "").asInstanceOf[String]

		if(objectType.equalsIgnoreCase("AssessmentItem")) {
			val row: Row = getAssessmentItemData(identifier, config)(cassandraUtil)
			val extProps = config.getConfig.getStringList("cassandra_fields_to_migrate.assessmentitem").asScala.toList
			val data: Map[String, String] = if (null != row) extProps.map(prop => prop -> row.getString(prop.toLowerCase())).toMap.filter(p => StringUtils.isNotBlank(p._2)) else Map[String, String]()
			logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: Fetched Cassandra data:: $data""")
			val migrateData = data.flatMap(rec => {
				Map(rec._1 -> StringUtils.replaceEach(rec._2, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String])))
			})
			updateAssessmentItemData(identifier, migrateData, config)(cassandraUtil)
			logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: Migrated Cassandra data:: $migrateData""")
		}

		if(objectType.equalsIgnoreCase("Content") && mimeType.equalsIgnoreCase("application/vnd.ekstep.ecml-archive")) {
			val ecmlBody: String = getContentBody(identifier, config)(cassandraUtil)
			logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: ECML Fetched body:: $ecmlBody""")

			val migratedECMLBody: String = extractAndValidateUrls(identifier, ecmlBody, config, httpUtil, cloudStorageUtil)

			updateContentBody(identifier, migratedECMLBody, config)(cassandraUtil)
			logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: ECML Migrated body:: $migratedECMLBody""")
		}

		if(objectType.equalsIgnoreCase("Collection") && !(status.equalsIgnoreCase("Live") ||
			status.equalsIgnoreCase("Unlisted")) && mimeType.equalsIgnoreCase("application/vnd.ekstep.content-collection")) {
			val collectionHierarchy: String = getCollectionHierarchy(identifier, config)(cassandraUtil)
			logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: Fetched Hierarchy:: $collectionHierarchy""")
			val migratedCollectionHierarchy: String = extractAndValidateUrls(identifier, collectionHierarchy, config, httpUtil, cloudStorageUtil)
			updateCollectionHierarchy(identifier, migratedCollectionHierarchy, config)(cassandraUtil)
			logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: Migrated Hierarchy:: $migratedCollectionHierarchy""")
		}

	}


	def extractAndValidateUrls(identifier: String, contentString: String, config: CSPMigratorConfig, httpUtil: HttpUtil, cloudStorageUtil: CloudStorageUtil): String = {
		val extractedUrls: List[String] = extarctUrls(contentString)

		if(extractedUrls.nonEmpty) {
			extractedUrls.toSet.foreach(urlString => {
				config.keyValueMigrateStrings.keySet().toArray().map(migrateDomain => {
					if(urlString.contains(migrateDomain)) {
						val migrateValue: String = StringUtils.replaceEach(urlString, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
						if(httpUtil.getSize(migrateValue) < 0) {
							if (config.copyMissingFiles) {
								// code to download file from old cloud path and upload to new cloud path
								val downloadedFile: File = downloadFile(s"/tmp/$identifier", urlString)
								val folderName: String = ""
								cloudStorageUtil.uploadFile(folderName,downloadedFile)

							} else throw new ServerException("ERR_NEW_PATH_NOT_FOUND", "File not found in the new path to migrate: " + migrateValue)
						}
					}
				})
			})
			StringUtils.replaceEach(contentString, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
		} else contentString
	}


	def downloadFile(downloadPath: String, fileUrl: String): File = try {
		createDirectory(downloadPath)
		val file = new File(downloadPath + File.separator + FilenameUtils.getName(fileUrl))
		FileUtils.copyURLToFile(new URL(fileUrl), file)
		file
	} catch {
		case e: IOException =>
			e.printStackTrace()
			throw new ServerException("ERR_INVALID_FILE_URL", "File not found in the old path to migrate: " + downloadPath)
	}

	def createDirectory(directoryName: String): Unit = {
		val theDir = new File(directoryName)
		if (!theDir.exists) theDir.mkdirs
	}



}
