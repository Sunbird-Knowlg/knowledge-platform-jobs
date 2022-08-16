package org.sunbird.job.migration.helpers

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.sunbird.job.task.CassandraDataMigrationConfig
import org.sunbird.job.util.CassandraUtil
import org.sunbird.spec.BaseTestSpec

class CassandraDataMigratorSpec extends BaseTestSpec {

  val config: Config = ConfigFactory.load("test.conf")
  var cassandraUtil: CassandraUtil = _
  val jobConfig: CassandraDataMigrationConfig = new CassandraDataMigrationConfig(config)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort)
    val session = cassandraUtil.session

    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    // Clear the metrics
    testCassandraUtil(cassandraUtil)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => {
      }
    }
  }


  "CassandraDataMigrator " should "update the string " in {
    val content_id = "do_4567"
    new TestCassandraDataMigrator().migrateData(jobConfig)(cassandraUtil)
    val row = new TestCassandraDataMigrator().readColumnDataFromCassandra(content_id, jobConfig)(cassandraUtil)
    val migratedData: String = row.getString(jobConfig.columnToMigrate)
   assert(migratedData.contains("\""+jobConfig.keyValueMigrateStrings.values().toArray().head))
  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }

  class TestCassandraDataMigrator extends CassandraDataMigrator {}
}
