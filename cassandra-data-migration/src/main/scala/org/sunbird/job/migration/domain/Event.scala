package org.sunbird.job.migration.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest
import org.sunbird.job.task.CassandraDataMigrationConfig

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

	private val jobName = "cassandra-data-migration"

	def eData: Map[String, AnyRef] = readOrDefault("edata", Map()).asInstanceOf[Map[String, AnyRef]]

	def action: String = readOrDefault[String]("edata.action", "")

	def keyspace: String = readOrDefault[String]("edata.keyspace", "")

	def table: String = readOrDefault[String]("edata.table", "")

	def column: String = readOrDefault[String]("edata.column", "")

	def isValid(config: CassandraDataMigrationConfig): Boolean = {
		((StringUtils.equals("migrate-cassandra", action) && StringUtils.isNotBlank(column))
			&& (config.cassandraKeyspace.contains(keyspace) && config.cassandraTable.contains(table))
			&& !StringUtils.equalsIgnoreCase(column, config.columnToMigrate))
	}

}