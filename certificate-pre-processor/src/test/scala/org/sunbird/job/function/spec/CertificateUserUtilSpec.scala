package org.sunbird.job.function.spec

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.{any, endsWith}
import org.mockito.Mockito.when
import org.sunbird.job.Metrics
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.{CertificateApiService, CertificateUserUtil}
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

class CertificateUserUtilSpec extends BaseTestSpec{

  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig = new CertificatePreProcessorConfig(config)
  val mockHttpUtil = mock[HttpUtil]
  val metrics = Metrics(new ConcurrentHashMap[String, AtomicLong]() {{
    put(jobConfig.dbReadCount, new AtomicLong())
    put(jobConfig.skippedEventCount, new AtomicLong())
  }})
  var cassandraUtil: CassandraUtil = _
  lazy private val gson = new Gson()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    testCassandraUtil(cassandraUtil)
    BaseMetricsReporter.gaugeMetrics.clear()
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

  it should "getUserIdsBasedOnCriteria" in {
    metrics.reset(jobConfig.dbReadCount)
    mockAll()
    val template = gson.fromJson(EventFixture.USER_UTIL_TEMPLATE, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    val edata = gson.fromJson(EventFixture.USER_UTIL_EDATA, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    val list = CertificateUserUtil.getUserIdsBasedOnCriteria(template, edata)(metrics, cassandraUtil, jobConfig)
    list should contain("user001")
    metrics.get(s"${jobConfig.dbReadCount}") should be(1)
  }

  it should "throw exception with empty criteria for getUserIdsBasedOnCriteria" in intercept[Exception] {
    mockAll()
    val template = gson.fromJson(EventFixture.USER_UTIL_TEMPLATE_2, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    val edata = gson.fromJson(EventFixture.USER_UTIL_EDATA, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    CertificateUserUtil.getUserIdsBasedOnCriteria(template, edata)(metrics, cassandraUtil, jobConfig)
  }

  it should "throw exception with invalid criteria for getUserIdsBasedOnCriteria" in intercept[Exception] {
    mockAll()
    val template = gson.fromJson(EventFixture.USER_UTIL_TEMPLATE_3, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    val edata = gson.fromJson(EventFixture.USER_UTIL_EDATA, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    CertificateUserUtil.getUserIdsBasedOnCriteria(template, edata)(metrics, cassandraUtil, jobConfig)
  }

  private def mockAll(): Unit = {
    CertificateApiService.httpUtil = mockHttpUtil
    when(mockHttpUtil.post(endsWith("/v1/search"), any[String])).thenReturn(HTTPResponse(200, """{"id":"api.user.search","ver":"v1","ts":"2020-10-24 14:24:57:555+0000","params":{"resmsgid":null,"msgid":"df3342f1082aa191128453838fb4e61f","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"count":1,"content":[{"firstName":"Reviewer","lastName":"User","maskedPhone":"******7418","rootOrgName":"Sunbird","userName":"ntptest103","rootOrgId":"ORG_001"}]}}""".stripMargin))
  }

  private def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }


}
