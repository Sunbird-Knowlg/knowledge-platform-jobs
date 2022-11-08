package org.sunbird.job.cspmigrator.helpers

import com.datastax.driver.core.Row
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.exception.ServerException
import org.sunbird.job.util._

import scala.collection.JavaConverters._

trait CSPMigrator extends MigrationObjectReader with MigrationObjectUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[CSPMigrator])

	def process(objMetadata: Map[String, AnyRef], status: String, config: CSPMigratorConfig, httpUtil: HttpUtil, neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil): Map[String, AnyRef] = {

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

		logger.info(s"""CSPMigrator:: process:: starting neo4j fields migration for $identifier - $objectType fields:: $fieldsToMigrate""")
		val migratedMetadataFields: Map[String, String] =  fieldsToMigrate.flatMap(migrateField => {
			if(objMetadata.contains(migrateField)) {
				val metadataField = objMetadata.getOrElse(migrateField, "").asInstanceOf[String]
				val migrateValue: String = StringUtils.replaceEach(metadataField, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))

				if(httpUtil.getSize(migrateValue) < 0) throw new ServerException("ERR_NEW_PATH_NOT_FOUND", "File not found in the new path to migrate: " + migrateValue)

				Map(migrateField -> migrateValue)
			} else Map.empty[String, String]
		}).filter(record => record._1.nonEmpty).toMap[String, String]

		if(objectType.equalsIgnoreCase("AssessmentItem")) {
			val row: Row = getAssessmentItemData(identifier, config)(cassandraUtil)
			val extProps = config.getConfig.getStringList("cassandra_fields_to_migrate.assessmentitem").asScala.toList
			val data: Map[String, String] = if (null != row) extProps.map(prop => prop -> row.getString(prop.toLowerCase())).toMap.filter(p => StringUtils.isNotBlank(p._2)) else Map[String, String]()
			logger.info(s"""CSPMigrator:: process:: $identifier - $objectType :: Fetched Cassandra data:: $data""")
			val migrateData = data.flatMap(rec => {
				Map(rec._1 -> StringUtils.replaceEach(rec._2, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String])))
			})
			updateAssessmentItemData(identifier, migrateData, config)(cassandraUtil)
			logger.info(s"""CSPMigrator:: process:: $identifier - $objectType :: Migrated Cassandra data:: $migrateData""")
		}

		if(objectType.equalsIgnoreCase("Content") && mimeType.equalsIgnoreCase("application/vnd.ekstep.ecml-archive")) {
			val ecmlBody: String = getContentBody(identifier, config)(cassandraUtil)
			logger.info(s"""CSPMigrator:: process:: $identifier - $objectType :: ECML Fetched body:: $ecmlBody""")
			val migratedECMLBody: String = StringUtils.replaceEach(ecmlBody, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
			updateContentBody(identifier, migratedECMLBody, config)(cassandraUtil)
			logger.info(s"""CSPMigrator:: process:: $identifier - $objectType :: ECML Migrated body:: $migratedECMLBody""")
		}

		if(objectType.equalsIgnoreCase("Collection") && !(status.equalsIgnoreCase("Live") ||
			status.equalsIgnoreCase("Unlisted")) && mimeType.equalsIgnoreCase("application/vnd.ekstep.content-collection")) {
			val collectionHierarchy: String = getCollectionHierarchy(identifier, config)(cassandraUtil)
			logger.info(s"""CSPMigrator:: process:: $identifier - $objectType :: Fetched Hierarchy:: $collectionHierarchy""")
			val migratedCollectionHierarchy: String = StringUtils.replaceEach(collectionHierarchy, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
			updateCollectionHierarchy(identifier, migratedCollectionHierarchy, config)(cassandraUtil)
			logger.info(s"""CSPMigrator:: process:: $identifier - $objectType :: Migrated Hierarchy:: $migratedCollectionHierarchy""")
		}

		migratedMetadataFields
	}

}
