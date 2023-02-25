package org.sunbird.job.cspmigrator.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.exception.ServerException
import org.sunbird.job.util.{CloudStorageUtil, HttpUtil}

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
			if(objMetadata.contains(migrateField) && (!migrateField.equalsIgnoreCase("streamingUrl") || !streamableMimeTypes.contains(mimeType)) && (mimeType.equalsIgnoreCase("video/x-youtube") && migrateField.equalsIgnoreCase("appIcon"))) {
				val metadataFieldValue = objMetadata.getOrElse(migrateField, "").asInstanceOf[String]

				val tempMetadataFieldValue = handleExternalURLS(metadataFieldValue,identifier, config, httpUtil, cloudStorageUtil)

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
}
