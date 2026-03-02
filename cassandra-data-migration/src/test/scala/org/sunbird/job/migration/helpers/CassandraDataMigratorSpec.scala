package org.sunbird.job.migration.helpers

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.anyString
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.task.CassandraDataMigrationConfig
import org.sunbird.job.util.CassandraUtil
import org.sunbird.spec.BaseTestSpec
import com.datastax.driver.core.{Session, Row}
import org.sunbird.job.migration.domain.Event
import scala.collection.JavaConverters._

class CassandraDataMigratorSpec extends BaseTestSpec with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: CassandraDataMigrationConfig = new CassandraDataMigrationConfig(config)
  implicit val cassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    Mockito.when(cassandraUtil.session).thenReturn(mock[Session])
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "CassandraDataMigrator" should "read primary keys from cassandra" in {
    val event = new Event(Map[String, Any]("eid" -> "BE_JOB_REQUEST", "ets" -> 1605816926271L, "mid" -> "LP.1605816926271.d6b1d8c8-7a4a-483a-b036-d6b1d8c87a4a", "actor" -> Map("id" -> "Cassandra Data Migration Job", "type" -> "System").asJava, "context" -> Map("pdata" -> Map("ver" -> "1.0", "id" -> "org.ekstep.platform").asJava, "channel" -> "sunbird", "env" -> "dev").asJava, "object" -> Map("ver" -> "1.0", "id" -> "do_123").asJava, "edata" -> Map("action" -> "cassandra-data-migration", "iteration" -> 1, "keyspace" -> "dev_question_store", "table" -> "question_data", "column" -> "body", "columnType" -> "text", "primaryKeyColumn" -> "identifier", "primaryKeyColumnType" -> "text").asJava).asJava, 0, 10)
    when(cassandraUtil.find(anyString())).thenReturn(new java.util.ArrayList[Row]())
    new TestCassandraDataMigrator().readPrimaryKeysFromCassandra(event)(cassandraUtil)
    Mockito.verify(cassandraUtil, Mockito.times(1)).find(anyString())
  }

}

class TestCassandraDataMigrator extends CassandraDataMigrator {
}
