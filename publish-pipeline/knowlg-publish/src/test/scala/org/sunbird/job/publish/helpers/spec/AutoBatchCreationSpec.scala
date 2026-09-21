package org.sunbird.job.publish.helpers.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.{anyString, contains}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.knowlg.publish.helpers.AutoBatchCreation
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil}

class AutoBatchCreationSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)
  var cassandraUtil: CassandraUtil = _
  val mockHttpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())

  val trackableEnabled = """{"enabled":"Yes","autoBatch":"Yes"}"""
  val trackableDisabled = """{"enabled":"No","autoBatch":"No"}"""

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort, jobConfig)
    val dataLoader = new CQLDataLoader(cassandraUtil.session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      if (cassandraUtil != null) cassandraUtil.close()
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case _: Exception =>
    }
  }

  "isTrackable" should "return true when tracking and autoBatch are both enabled" in {
    val metadata = Map[String, AnyRef]("trackable" -> trackableEnabled)
    new TestAutoBatchCreation().isTrackable(metadata, "do_123") should be(true)
  }

  it should "return false when trackable metadata is disabled" in {
    val metadata = Map[String, AnyRef]("trackable" -> trackableDisabled)
    new TestAutoBatchCreation().isTrackable(metadata, "do_123") should be(false)
  }

  it should "return false rather than throw when metadata is empty" in {
    new TestAutoBatchCreation().isTrackable(Map.empty[String, AnyRef], "do_123") should be(false)
  }

  "isBatchExists" should "return true for an identifier with a seeded active Open batch" in {
    new TestAutoBatchCreation().isBatchExists("do_11300581751853056018")(jobConfig, cassandraUtil) should be(true)
  }

  it should "return false for an identifier with no batch rows" in {
    new TestAutoBatchCreation().isBatchExists("do_11300581751853056099")(jobConfig, cassandraUtil) should be(false)
  }

  it should "return false for a batch row with a null status, treating it as inactive rather than active" in {
    new TestAutoBatchCreation().isBatchExists("do_11300581751853056077")(jobConfig, cassandraUtil) should be(false)
  }

  "getAutoBatchDetails" should "return a populated map when trackable and no active batch exists" in {
    val obj = new ObjectData("do_11300581751853056099", Map[String, AnyRef]("name" -> "Test Course", "createdBy" -> "user1", "trackable" -> trackableEnabled))
    val result = new TestAutoBatchCreation().getAutoBatchDetails(obj)(cassandraUtil, jobConfig)
    result.isEmpty should be(false)
    result.get("identifier") should be("do_11300581751853056099")
    result.get("name") should be("Test Course")
  }

  it should "return an empty map when trackable is disabled" in {
    val obj = new ObjectData("do_11300581751853056099", Map[String, AnyRef]("name" -> "Test Course", "trackable" -> trackableDisabled))
    val result = new TestAutoBatchCreation().getAutoBatchDetails(obj)(cassandraUtil, jobConfig)
    result.isEmpty should be(true)
  }

  it should "return an empty map when an active batch already exists" in {
    val obj = new ObjectData("do_11300581751853056018", Map[String, AnyRef]("name" -> "Test Course", "trackable" -> trackableEnabled))
    val result = new TestAutoBatchCreation().getAutoBatchDetails(obj)(cassandraUtil, jobConfig)
    result.isEmpty should be(true)
  }

  "createBatch" should "succeed without throwing when the LMS API returns 200" in {
    when(mockHttpUtil.post(anyString(), anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(HTTPResponse(200, "{}"))
    val eData = new java.util.HashMap[String, AnyRef]() {
      {
        put("identifier", "do_11300581751853056099")
        put("name", "Test Course")
      }
    }
    new TestAutoBatchCreation().createBatch(eData, "2026-09-21")(jobConfig, mockHttpUtil)
    org.mockito.Mockito.verify(mockHttpUtil).post(org.mockito.ArgumentMatchers.eq(jobConfig.autoBatchCreateAPIPath), contains("\"courseId\":\"do_11300581751853056099\""), org.mockito.ArgumentMatchers.any())
  }

  it should "throw when the LMS API returns a non-200 response" in {
    when(mockHttpUtil.post(anyString(), anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(HTTPResponse(500, "error"))
    val eData = new java.util.HashMap[String, AnyRef]() {
      {
        put("identifier", "do_11300581751853056099")
        put("name", "Test Course")
      }
    }
    an[Exception] should be thrownBy {
      new TestAutoBatchCreation().createBatch(eData, "2026-09-21")(jobConfig, mockHttpUtil)
    }
  }
}

class TestAutoBatchCreation extends AutoBatchCreation {}
