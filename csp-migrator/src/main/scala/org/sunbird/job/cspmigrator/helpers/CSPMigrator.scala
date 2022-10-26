package org.sunbird.job.cspmigrator.helpers

import com.datastax.driver.core.Row
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.domain.Event
import org.sunbird.job.cspmigrator.models.{ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.exception.ServerException
import org.sunbird.job.util._

import scala.collection.JavaConverters._

trait CSPMigrator extends ObjectReader with ObjectUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[CSPMigrator])

	def process(obj: ObjectData, config: CSPMigratorConfig, readerConfig: ExtDataConfig, event: Event, neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil): Unit = {

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
0
		val status: String = obj.metadata.getOrElse("status","").asInstanceOf[String]
		val objectType: String = obj.metadata.getOrElse("objectType","").asInstanceOf[String]
		val mimeType: String = obj.metadata.getOrElse("mimeType","").asInstanceOf[String]
		val fieldsToMigrate: List[String] = if (config.getConfig.hasPath("neo4j_fields_to_migrate."+objectType.toLowerCase())) config.getConfig.getStringList("neo4j_fields_to_migrate."+objectType.toLowerCase()).asScala.toList
													else throw new ServerException("ERR_CONFIG_NOT_FOUND", "Fields to migrate configuration not found for objectType: " + objectType)

		if(objectType.equalsIgnoreCase("AssessmentItem")) {
			val row: Row = getQuestionData(obj.dbId, config)(cassandraUtil)
			val data: Map[String, String] = if (null != row) extProps.map(prop => prop -> row.getString(prop.toLowerCase())).toMap.filter(p => StringUtils.isNotBlank(p._2)) else Map[String, String]()

			val updatedData = data.flatMap(rec => {
				Map(rec._1 -> StringUtils.replaceEach(rec._2, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String])))
			})

			updateQuestionData(obj.dbId, updatedData, config)(cassandraUtil)
		}

		if(objectType.equalsIgnoreCase("Content") && mimeType.equalsIgnoreCase("application/vnd.ekstep.ecml-archive")) {
			val ecmlBody: String = getContentBody(obj.dbId, readerConfig)(cassandraUtil)
			val migratedECMLBody: String = StringUtils.replaceEach(ecmlBody, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
			updateContentBody(obj.dbId, migratedECMLBody, readerConfig)(cassandraUtil)
		}

		val migratedMetadataFields: Map[String, String] =  fieldsToMigrate.flatMap(migrateField => {
			if(obj.metadata.contains(migrateField)) {
				val metadataField = obj.metadata.getOrElse(migrateField, "").asInstanceOf[String]
				Map(migrateField -> StringUtils.replaceEach(metadataField, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String])))
			} else Map.empty[String, String]
		}).filter(record => record._1.nonEmpty).toMap[String, String]

		neo4JUtil.updateNode(obj.identifier, obj.metadata ++ migratedMetadataFields + ("migrationVersion" -> 1.0.asInstanceOf[Number]))


		if(!(status.equalsIgnoreCase("Live") || status.equalsIgnoreCase("Unlisted")) && obj.identifier.endsWith(".img")) {
			val liveObj = getLiveNode(event.identifier, obj.pkgVersion, obj.mimeType, readerConfig)(neo4JUtil, cassandraUtil)

			if(objectType.equalsIgnoreCase("AssessmentItem")) {
				val row: Row = getQuestionData(event.identifier, config)(cassandraUtil)
				val data: Map[String, String] = if (null != row) extProps.map(prop => prop -> row.getString(prop.toLowerCase())).toMap.filter(p => StringUtils.isNotBlank(p._2)) else Map[String, String]()

				val updatedData = data.flatMap(rec => {
					Map(rec._1 -> StringUtils.replaceEach(rec._2, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String])))
				})

				updateQuestionData(event.identifier, updatedData, config)(cassandraUtil)
			}

			if(objectType.equalsIgnoreCase("Content") && mimeType.equalsIgnoreCase("application/vnd.ekstep.ecml-archive")) {
				val ecmlBody: String = getContentBody(event.identifier, readerConfig)(cassandraUtil)
				val migratedECMLBody: String = StringUtils.replaceEach(ecmlBody, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
				updateContentBody(event.identifier, migratedECMLBody, readerConfig)(cassandraUtil)
			}

			val migratedLiveMetadataFields: Map[String, String] =  fieldsToMigrate.flatMap(migrateField => {
				if(liveObj.metadata.contains(migrateField)) {
					val metadataField = liveObj.metadata.getOrElse(migrateField, "").asInstanceOf[String]
					Map(migrateField -> StringUtils.replaceEach(metadataField, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String])))
				} else Map.empty[String, String]
			}).filter(record => record._1.nonEmpty).toMap[String, String]

			neo4JUtil.updateNode(liveObj.identifier, liveObj.metadata ++ migratedLiveMetadataFields + ("migrationVersion" -> 1.0.asInstanceOf[Number]))
		}


		logger.info("CSPMigrator :: process :: identifier: " + event.identifier )

	}

	override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

	override def getExtData(identifier: String, pkgVersion: Double, mimeType: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[ObjectExtData] = None

	override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

	override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None
}
