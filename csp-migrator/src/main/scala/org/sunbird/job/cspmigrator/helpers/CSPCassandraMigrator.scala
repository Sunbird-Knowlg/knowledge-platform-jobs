package org.sunbird.job.cspmigrator.helpers

import com.datastax.driver.core.Row
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.cspmigrator.task.CSPMigratorConfig
import org.sunbird.job.util._

import scala.collection.JavaConverters._

trait CSPCassandraMigrator extends MigrationObjectReader with MigrationObjectUpdater{

	private[this] val logger = LoggerFactory.getLogger(classOf[CSPCassandraMigrator])

	def process(objMetadata: Map[String, AnyRef], config: CSPMigratorConfig, httpUtil: HttpUtil, cassandraUtil: CassandraUtil, cloudStorageUtil: CloudStorageUtil): Unit = {

		val objectType: String = objMetadata.getOrElse("objectType","").asInstanceOf[String]
		val identifier: String = objMetadata.getOrElse("identifier", "").asInstanceOf[String]

		objectType match {
			case "AssessmentItem" =>
				val row: Row = getAssessmentItemData(identifier, config)(cassandraUtil)
				val extProps = config.getConfig.getStringList("cassandra_fields_to_migrate.assessmentitem").asScala.toList
				val data: Map[String, String] = if (null != row) extProps.map(prop => prop -> row.getString(prop.toLowerCase())).toMap.filter(p => StringUtils.isNotBlank(p._2)) else Map[String, String]()
				logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: Fetched Cassandra data:: $data""")
				val migrateData = data.flatMap(rec => {
					Map(rec._1 -> StringUtils.replaceEach(rec._2, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String])))
				})
				updateAssessmentItemData(identifier, migrateData, config)(cassandraUtil)
				logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: Migrated Cassandra data:: $migrateData""")
			case 	"Content" | "ContentImage" =>
				val ecmlBody: String = getContentBody(identifier, config)(cassandraUtil)
				logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: ECML Fetched body:: $ecmlBody""")
				val migratedECMLBody: String = extractAndValidateUrls(identifier, ecmlBody, config, httpUtil, cloudStorageUtil)
				updateContentBody(identifier, migratedECMLBody, config)(cassandraUtil)
				logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: ECML Migrated body:: $migratedECMLBody""")
			case 	"Collection" | "CollectionImage" =>
				val collectionHierarchy: String = getCollectionHierarchy(identifier, config)(cassandraUtil)
				logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: Fetched Hierarchy:: $collectionHierarchy""")
				if(collectionHierarchy != null && collectionHierarchy.nonEmpty) {
					val migratedCollectionHierarchy: String = extractAndValidateUrls(identifier, collectionHierarchy, config, httpUtil, cloudStorageUtil, false)
					updateCollectionHierarchy(identifier, migratedCollectionHierarchy, config)(cassandraUtil)
					logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: Migrated Hierarchy:: $migratedCollectionHierarchy""")
				}
			case "QuestionSet" | "QuestionSetImage" => {
				val qsH: String = getQuestionSetHierarchy(identifier, config)(cassandraUtil)
				logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: Fetched Hierarchy:: $qsH""")
				val migratedQsHierarchy: String = extractAndValidateUrls(identifier, qsH, config, httpUtil, cloudStorageUtil)
				updateQuestionSetHierarchy(identifier, migratedQsHierarchy, config)(cassandraUtil)
				logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: Migrated Hierarchy:: $migratedQsHierarchy""")
			}
			case _ => logger.info(s"""CSPCassandraMigrator:: process:: $identifier - $objectType :: NO CASSANDRA MIGRATION PERFORMED!! """)
		}
	}
}