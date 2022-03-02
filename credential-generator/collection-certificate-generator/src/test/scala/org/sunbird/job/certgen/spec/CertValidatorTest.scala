package org.sunbird.job.certgen.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.sunbird.job.Metrics
import org.sunbird.job.certgen.domain.Event
import org.sunbird.job.certgen.exceptions.ValidationException
import org.sunbird.job.certgen.fixture.EventFixture
import org.sunbird.job.certgen.functions.CertValidator
import org.sunbird.job.certgen.task.CertificateGeneratorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec

import java.util

class CertValidatorTest extends BaseTestSpec{
  var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: CertificateGeneratorConfig = new CertificateGeneratorConfig(config)
  val httpUtil: HttpUtil = new HttpUtil
  val mockHttpUtil:HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  val metricJson = s"""{"${jobConfig.enrollmentDbReadCount}": 0, "${jobConfig.skippedEventCount}": 0}"""
  val mockMetrics = mock[Metrics](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
    val session = cassandraUtil.session
    session.execute(s"DROP KEYSPACE IF EXISTS ${jobConfig.dbKeyspace}")
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))

  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }
  
  "CertValidator isNotIssued" should "return true if re-Issued" in {
    val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventFixture.EVENT_1), 0, 0)
    val isCertificateIssued = new CertValidator().isNotIssued(event)(jobConfig, mockMetrics, cassandraUtil)
    assert(true == isCertificateIssued)
  }

  "CertValidator isNotIssued" should "return false if already issued" in {
    val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventFixture.EVENT_3), 0, 0)
    val isCertificateIssued = new CertValidator().isNotIssued(event)(jobConfig, mockMetrics, cassandraUtil)
    assert(false == isCertificateIssued)
  }

  "CertValidator with enabled suppress exception on signatoryList with empty field values" should "not throw exception" in {
    val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventFixture.EVENT_4), 0, 0)
    noException should be thrownBy new CertValidator().validateGenerateCertRequest(event, true)
  }

  "CertValidator on signatoryList with empty field values" should " throw exception" in {
    val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventFixture.EVENT_4), 0, 0)
    an [ValidationException] should be thrownBy new CertValidator().validateGenerateCertRequest(event, false)
  }

}
