package org.sunbird.job.migration.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.task.CassandraDataMigrationConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.migration.domain.Event
import org.sunbird.job.migration.helpers.CassandraDataMigrator
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util

class CassandraDataMigrationFunction(config: CassandraDataMigrationConfig,
                                     @transient var neo4JUtil: Neo4JUtil = null,
                                     @transient var cassandraUtil: CassandraUtil = null,
                                     @transient var cloudStorageUtil: CloudStorageUtil = null)
                                    (implicit mapTypeInfo: TypeInformation[util.Map[String, AnyRef]], stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) with CassandraDataMigrator {

  private[this] lazy val logger = LoggerFactory.getLogger(classOf[CassandraDataMigrationFunction])
  lazy val defCache: DefinitionCache = new DefinitionCache()

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.skippedEventCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort)
  }

  override def close(): Unit = {
    super.close()
    cassandraUtil.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    if (event.isValid(config))
      migrateData(config)(cassandraUtil)
  }
}
