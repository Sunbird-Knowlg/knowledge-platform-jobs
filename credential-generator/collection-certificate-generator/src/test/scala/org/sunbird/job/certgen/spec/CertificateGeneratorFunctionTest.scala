package org.sunbird.job.certgen.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.sunbird.incredible.StorageParams
import org.sunbird.incredible.processor.store.StorageService
import org.sunbird.job.Metrics
import org.sunbird.job.certgen.domain.Event
import org.sunbird.job.certgen.fixture.EventFixture
import org.sunbird.job.certgen.functions.{CertValidator, CertificateGeneratorFunction}
import org.sunbird.job.certgen.task.CertificateGeneratorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec

class CertificateGeneratorFunctionTest extends BaseTestSpec {

  var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: CertificateGeneratorConfig = new CertificateGeneratorConfig(config)
  val httpUtil: HttpUtil = new HttpUtil
  val mockHttpUtil:HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
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

  "Certificate generation process " should " not throw exception on disabled validation for signatorylist empty field values" in {
    val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventFixture.EVENT_4), 0, 0)
    noException should be thrownBy new CertificateGeneratorFunction(jobConfig, httpUtil, storageService, cassandraUtil).processElement(event, null, mockMetrics)
  }

}
