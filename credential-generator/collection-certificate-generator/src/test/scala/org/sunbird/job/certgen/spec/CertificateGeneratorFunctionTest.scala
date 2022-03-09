package org.sunbird.job.certgen.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.incredible.{JsonKeys, ScalaModuleJsonUtils, StorageParams}
import org.sunbird.incredible.processor.store.StorageService
import org.sunbird.job.Metrics
import org.sunbird.job.certgen.domain.Event
import org.sunbird.job.certgen.exceptions.ServerException
import org.sunbird.job.certgen.fixture.EventFixture
import org.sunbird.job.certgen.functions.{CertValidator, CertificateGeneratorFunction}
import org.sunbird.job.certgen.task.CertificateGeneratorConfig
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec

class CertificateGeneratorFunctionTest extends BaseTestSpec {

  var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: CertificateGeneratorConfig = new CertificateGeneratorConfig(config)
  val httpUtil: HttpUtil = new HttpUtil
  val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  val storageParams: StorageParams = StorageParams(jobConfig.storageType, jobConfig.azureStorageKey, jobConfig.azureStorageSecret, jobConfig.containerName)
  val storageService: StorageService = new StorageService(storageParams)
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

  "Certificate generation process " should " not throw exception on enabled suppress exception for signatorylist with empty field values" in {
    val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventFixture.EVENT_4), 0, 0)
    noException should be thrownBy new CertificateGeneratorFunction(jobConfig, httpUtil, storageService, cassandraUtil).processElement(event, null, mockMetrics)
  }

  "Certificate rc read api call for valid identifier " should " not throw serverException " in {
    when(mockHttpUtil.get(any[String], any[Map[String, String]])).thenReturn(HTTPResponse(200, """{}"""))
    noException should be thrownBy new CertificateGeneratorFunction(jobConfig, mockHttpUtil, storageService, cassandraUtil).callCertificateRc(jobConfig.rcReadApi, "validId", null)
  }

  "Certificate rc read api call for invalid identifier " should " throw serverException " in {
    when(mockHttpUtil.get(any[String], any[Map[String, String]])).thenReturn(HTTPResponse(500, """{}"""))
    an [ServerException] should be thrownBy new CertificateGeneratorFunction(jobConfig, mockHttpUtil, storageService, cassandraUtil).callCertificateRc(jobConfig.rcReadApi, "invalidId", null)
  }

  "Certificate rc delete api call for valid identifier " should " not throw serverException " in {
    when(mockHttpUtil.delete(any[String])).thenReturn(HTTPResponse(200, """{}"""))
    noException should be thrownBy new CertificateGeneratorFunction(jobConfig, mockHttpUtil, storageService, cassandraUtil).callCertificateRc(jobConfig.rcDeleteApi, "validId", null)
  }

  "Certificate rc delete api call for invalid identifier " should " throw serverException " in {
    when(mockHttpUtil.delete(any[String])).thenReturn(HTTPResponse(500, """{}"""))
    an [ServerException] should be thrownBy new CertificateGeneratorFunction(jobConfig, mockHttpUtil, storageService, cassandraUtil).callCertificateRc(jobConfig.rcDeleteApi, "invalidId", null)
  }

  "Certificate rc create api call for valid identifier " should " not throw serverException and returns validId" in {
    val certReq = Map[String, AnyRef](
      JsonKeys.NAME -> "name",
      JsonKeys.CERTIFICATE_NAME -> "name"
    )
    when(mockHttpUtil.post(jobConfig.rcBaseUrl + "/" + jobConfig.rcEntity + "/" + jobConfig.rcCreateApi, ScalaModuleJsonUtils.serialize(certReq))).thenReturn(HTTPResponse(200, """{"id":"sunbird-rc.registry.create","ver":"1.0","ets":1646765130993,"params":{"resmsgid":"","msgid":"cca2e242-fce7-47ec-b5d0-61cebe56c31d","err":"","status":"SUCCESSFUL","errmsg":""},"responseCode":"OK","result":{"TrainingCertificate":{"osid":"validId"}}}"""))
    var id: String = null
    noException should be thrownBy {
       id = new CertificateGeneratorFunction(jobConfig, mockHttpUtil, storageService, cassandraUtil).callCertificateRc(jobConfig.rcCreateApi, null,  certReq)
    }
    assert(id equals "validId")
  }

  "Certificate rc create api call for invalid identifier " should " throw serverException " in {
    val certReq = Map[String, AnyRef](
      JsonKeys.NAME -> "name",
      JsonKeys.CERTIFICATE_NAME -> "name"
    )
    when(mockHttpUtil.post(jobConfig.rcBaseUrl + "/" + jobConfig.rcEntity + "/" + jobConfig.rcCreateApi, ScalaModuleJsonUtils.serialize(certReq))).thenReturn(HTTPResponse(500, """{}"""))
    var id: String = null
    an [ServerException] should be thrownBy new CertificateGeneratorFunction(jobConfig, mockHttpUtil, storageService, cassandraUtil).callCertificateRc(jobConfig.rcCreateApi, null,  certReq)
  }


}
