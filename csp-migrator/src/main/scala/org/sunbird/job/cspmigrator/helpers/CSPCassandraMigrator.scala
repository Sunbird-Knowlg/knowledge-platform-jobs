package org.sunbird.job.cspmigrator.helpers

import com.datastax.driver.core.Row
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.exception.ServerException
import org.sunbird.job.util._

import scala.collection.JavaConverters._

trait CSPCassandraMigrator extends MigrationObjectReader with MigrationObjectUpdater with URLExtractor {

	private[this] val logger = LoggerFactory.getLogger(classOf[CSPCassandraMigrator])

	def process(objMetadata: Map[String, AnyRef], status: String, config: CSPMigratorConfig, httpUtil: HttpUtil, cassandraUtil: CassandraUtil): Unit = {

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

			val migratedECMLBody: String = extractAndValidateUrls(ecmlBody, config)

			updateContentBody(identifier, migratedECMLBody, config)(cassandraUtil)
			logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: ECML Migrated body:: $migratedECMLBody""")
		}

		if(objectType.equalsIgnoreCase("Collection") && !(status.equalsIgnoreCase("Live") ||
			status.equalsIgnoreCase("Unlisted")) && mimeType.equalsIgnoreCase("application/vnd.ekstep.content-collection")) {
			val collectionHierarchy: String = getCollectionHierarchy(identifier, config)(cassandraUtil)
			logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: Fetched Hierarchy:: $collectionHierarchy""")
			val migratedCollectionHierarchy: String = StringUtils.replaceEach(collectionHierarchy, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
			updateCollectionHierarchy(identifier, migratedCollectionHierarchy, config)(cassandraUtil)
			logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: Migrated Hierarchy:: $migratedCollectionHierarchy""")
		}

	}


	def extractAndValidateUrls(contentString: String, config: CSPMigratorConfig): String = {
		val extractedUrls: List[String] = extarctUrls(contentString)

		if(extractedUrls.nonEmpty) {
			extractedUrls.foreach(url => {
				url.contains()
			})

			StringUtils.replaceEach(contentString, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))
		} else contentString
	}

}
