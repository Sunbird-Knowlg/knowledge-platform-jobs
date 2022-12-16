package org.sunbird.job.migration.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.migration.domain.Event
import org.sunbird.job.task.CassandraDataMigrationConfig
import org.sunbird.job.util._

trait CassandraDataMigrator {

	private[this] val logger = LoggerFactory.getLogger(classOf[CassandraDataMigrator])

	def migrateData(event: Event, config: CassandraDataMigrationConfig)(implicit cassandraUtil: CassandraUtil): Unit = {

		// select primary key Column rows from table to migrate
		val primaryKeys = readPrimaryKeysFromCassandra(event)
		logger.info(s"CassandraDataMigrator:: migrateData:: After fetching primary keys. Keys Count:: " + primaryKeys.size())
		primaryKeys.forEach(col => {
			val primaryKey = event.primaryKeyColumnType.toLowerCase match {
				case "uuid" => col.getUUID(event.primaryKeyColumn)
				case _ => col.getString(event.primaryKeyColumn)
			}
			val row = readColumnDataFromCassandra(primaryKey, event)(cassandraUtil)
			if(row != null) {
				val fetchedData: String = row.getString(event.column)
				logger.info(s"CassandraDataMigrator:: migrateData:: Fetched ${event.column} in Cassandra For $primaryKey :: $fetchedData")

				val migratedData = StringUtils.replaceEach(fetchedData, config.keyValueMigrateStrings.keySet().toArray().map(_.asInstanceOf[String]), config.keyValueMigrateStrings.values().toArray().map(_.asInstanceOf[String]))

				// Pass updated data to row using primaryKey field
				updateMigratedDataToCassandra(migratedData, primaryKey, event) (cassandraUtil)
			}
		})

	}

	def readPrimaryKeysFromCassandra(event: Event)(implicit cassandraUtil: CassandraUtil): java.util.List[Row] = {
		val query =  s"""select ${event.primaryKeyColumn} from ${event.keyspace}.${event.table} ALLOW FILTERING;"""
		cassandraUtil.find(query)
	}

	def readColumnDataFromCassandra(primaryKey: AnyRef, event: Event)(implicit cassandraUtil: CassandraUtil): Row = {
		val query = event.primaryKeyColumnType.toLowerCase match {
			case "uuid" => event.columnType.toLowerCase match {
				case "blob" => s"""select blobAsText(${event.column}) as ${event.column} from ${event.keyspace}.${event.table} where ${event.primaryKeyColumn}=$primaryKey ALLOW FILTERING;"""
				case _ => s"""select ${event.column} from ${event.keyspace}.${event.table} where ${event.primaryKeyColumn}=$primaryKey ALLOW FILTERING;"""
			}
			case _ => event.columnType.toLowerCase match {
				case "blob" => s"""select blobAsText(${event.column}) as ${event.column} from ${event.keyspace}.${event.table} where ${event.primaryKeyColumn}='$primaryKey' ALLOW FILTERING;"""
				case _ => s"""select ${event.column} from ${event.keyspace}.${event.table} where ${event.primaryKeyColumn}='$primaryKey' ALLOW FILTERING;"""
			}
		}

		cassandraUtil.findOne(query)
	}

	def updateMigratedDataToCassandra(migratedData: String, primaryKey: AnyRef, event: Event)(implicit cassandraUtil: CassandraUtil): Unit = {
		val update = QueryBuilder.update(event.keyspace, event.table)
		val clause: Clause = QueryBuilder.eq(event.primaryKeyColumn, primaryKey)
		update.where.and(clause)
		event.columnType.toLowerCase match {
			case "blob" => update.`with`(QueryBuilder.set(event.column, QueryBuilder.fcall("textAsBlob", migratedData)))
			case _ => update.`with`(QueryBuilder.set(event.column, migratedData))
		}

		logger.info(s"CassandraDataMigrator:: updateMigratedDataToCassandra:: Updating ${event.column} in Cassandra For $primaryKey :: ${update}")
		val result = cassandraUtil.update(update)
		if (result) logger.info(s"CassandraDataMigrator:: updateMigratedDataToCassandra:: ${event.column} Updated Successfully For $primaryKey")
		else {
			logger.error(s"CassandraDataMigrator:: updateMigratedDataToCassandra:: ${event.column} Update Failed For $primaryKey")
			throw new InvalidInputException(s"${event.column} Update Failed For $primaryKey")
		}
	}

}
