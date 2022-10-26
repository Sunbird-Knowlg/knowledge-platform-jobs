package org.sunbird.job.cspmigrator.helpers

import com.datastax.driver.core.Row
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.domain.Event
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.exception.ServerException
import org.sunbird.job.util._

import scala.collection.JavaConverters._

trait CSPMigrator extends ObjectReader with ObjectUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[CSPMigrator])

	def process(objMetadata: Map[String, AnyRef], config: CSPMigratorConfig, event: Event, httpUtil: HttpUtil, neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil): Map[String, AnyRef] = {

		// fetch the objectType.
		// check if the object has only Draft OR only Live OR live/image if the objectType is content/collection.
		// fetch the string replace data from config
		// fetch the list of attributes for string replace from config
		// check the string of each property and perform string replace.
		// commit updated data to DB.
		// if objectType is Asset and mimeType is video, trigger streamingUrl generation - pending
		// if objectType is content and mimeType is ECML, need to update ECML content body
		// if objectType is collection, fetch hierarchy data and update cassandra data with the replaced string - pending
		// if objectType is content/collection and the node is Live, trigger the LiveNodePublisher flink job - pending
		// if objectType is AssessmentItem, migrate cassandra data as well
		// update the migrationVersion of the object

		// validation of the replace URL paths to be done. If not available, skip the content - pending
		// Failed contents set migrationVersion to 0.1 - pending
0
		val status: String = objMetadata.getOrElse("status","").asInstanceOf[String]
		val objectType: String = objMetadata.getOrElse("objectType","").asInstanceOf[String]
		val mimeType: String = objMetadata.getOrElse("mimeType","").asInstanceOf[String]
		val identifier: String = objMetadata.getOrElse("identifier", "").asInstanceOf[String]
		val fieldsToMigrate: List[String] = if (config.getConfig.hasPath("neo4j_fields_to_migrate."+objectType.toLowerCase())) config.getConfig.getStringList("neo4j_fields_to_migrate."+objectType.toLowerCase()).asScala.toList
													else throw new ServerException("ERR_CONFIG_NOT_FOUND", "Fields to migrate configuration not found for objectType: " + objectType)

		if(objectType.equalsIgnoreCase("AssessmentItem")) {
			val row: Row = getQuestionData(identifier, config)(cassandraUtil)
			val data: Map[String, String] = if (null != row) extProps.map(prop => prop -> row.getString(prop.toLowerCase())).toMap.filter(p => StringUtils.isNotBlank(p._2)) else Map[String, String]()

			val updatedData = data.flatMap(rec => {
				Map(rec._1 -> StringUtils.replaceEach(rec._2, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String])))
			})

			updateQuestionData(identifier, updatedData, config)(cassandraUtil)
		}

		if(objectType.equalsIgnoreCase("Content") && mimeType.equalsIgnoreCase("application/vnd.ekstep.ecml-archive")) {
			val ecmlBody: String = getContentBody(identifier, config)(cassandraUtil)
			val migratedECMLBody: String = StringUtils.replaceEach(ecmlBody, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
			updateContentBody(identifier, migratedECMLBody, config)(cassandraUtil)
		}

		if(objectType.equalsIgnoreCase("Collection") && mimeType.equalsIgnoreCase("application/vnd.ekstep.content-collection")) {
			val collectionHierarchy: String = getCollectionHierarchy(identifier, config)(cassandraUtil)
			val migratedCollectionHierarchy: String = StringUtils.replaceEach(collectionHierarchy, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
			updateCollectionHierarchy(identifier, migratedCollectionHierarchy, config)(cassandraUtil)
		}

		val migratedMetadataFields: Map[String, String] =  fieldsToMigrate.flatMap(migrateField => {
			if(objMetadata.contains(migrateField)) {
				val metadataField = objMetadata.getOrElse(migrateField, "").asInstanceOf[String]
				Map(migrateField -> StringUtils.replaceEach(metadataField, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String])))
			} else Map.empty[String, String]
		}).filter(record => record._1.nonEmpty).toMap[String, String]

		neo4JUtil.updateNode(identifier, objMetadata ++ migratedMetadataFields + ("migrationVersion" -> 1.0.asInstanceOf[Number]))


		if(!(status.equalsIgnoreCase("Live") || status.equalsIgnoreCase("Unlisted")) && identifier.endsWith(".img")) {
			val liveObjMetadata: Map[String, AnyRef] = getLiveNodeMetadata(event.identifier)(neo4JUtil)

			if(objectType.equalsIgnoreCase("AssessmentItem")) {
				val row: Row = getQuestionData(event.identifier, config)(cassandraUtil)
				val data: Map[String, String] = if (null != row) extProps.map(prop => prop -> row.getString(prop.toLowerCase())).toMap.filter(p => StringUtils.isNotBlank(p._2)) else Map[String, String]()

				val updatedData = data.flatMap(rec => {
					Map(rec._1 -> StringUtils.replaceEach(rec._2, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String])))
				})

				updateQuestionData(event.identifier, updatedData, config)(cassandraUtil)
			}

			if(objectType.equalsIgnoreCase("Content") && mimeType.equalsIgnoreCase("application/vnd.ekstep.ecml-archive")) {
				val ecmlBody: String = getContentBody(event.identifier, config)(cassandraUtil)
				val migratedECMLBody: String = StringUtils.replaceEach(ecmlBody, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
				updateContentBody(event.identifier, migratedECMLBody, config)(cassandraUtil)
			}

			val migratedLiveMetadataFields: Map[String, String] =  fieldsToMigrate.flatMap(migrateField => {
				if(liveObjMetadata.contains(migrateField)) {
					val metadataField = liveObjMetadata.getOrElse(migrateField, "").asInstanceOf[String]
					Map(migrateField -> StringUtils.replaceEach(metadataField, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String])))
				} else Map.empty[String, String]
			}).filter(record => record._1.nonEmpty).toMap[String, String]

			neo4JUtil.updateNode(liveObjMetadata.getOrElse("identifier","").asInstanceOf[String], liveObjMetadata ++ migratedLiveMetadataFields + ("migrationVersion" -> 1.0.asInstanceOf[Number]))

			liveObjMetadata

		} else objMetadata

	}




}
