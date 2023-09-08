package org.sunbird.job.migration.helpers

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.sunbird.job.migration.domain.Event
import org.sunbird.job.migration.fixture.EventFixture
import org.sunbird.job.task.CassandraDataMigrationConfig
import org.sunbird.job.util.{CassandraUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec

import java.util

class CassandraDataMigratorSpec extends BaseTestSpec {

  val config: Config = ConfigFactory.load("test.conf")
  var cassandraUtil: CassandraUtil = _
  val jobConfig: CassandraDataMigrationConfig = new CassandraDataMigrationConfig(config)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort, jobConfig)
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
    val event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1), 0, 10)
    new TestCassandraDataMigrator().migrateData(event, jobConfig)(cassandraUtil)
    val row = new TestCassandraDataMigrator().readColumnDataFromCassandra(content_id, event)(cassandraUtil)
    val migratedData: String = row.getString(event.column)
    assert(migratedData.contains("\""+jobConfig.keyValueMigrateStrings.values().toArray().head))
  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }

  class TestCassandraDataMigrator extends CassandraDataMigrator {}
}
