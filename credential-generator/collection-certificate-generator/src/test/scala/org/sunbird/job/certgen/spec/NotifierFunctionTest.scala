package org.sunbird.job.certgen.spec

import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito._
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}
import org.mockito.ArgumentMatchers.{any, endsWith}
import org.sunbird.job.Metrics
import org.sunbird.job.certgen.functions.{NotificationMetaData, NotifierFunction}
import org.sunbird.job.certgen.task.CertificateGeneratorConfig


class NotifierFunctionTest extends BaseTestSpec {
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf")
  val notifierConfig: CertificateGeneratorConfig = new CertificateGeneratorConfig(config)
  val mockHttpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  val metrics = Metrics(new ConcurrentHashMap[String, AtomicLong]() {
    {
      put(notifierConfig.courseBatchdbReadCount, new AtomicLong())
      put(notifierConfig.skipNotifyUserCount, new AtomicLong())
      put(notifierConfig.notifiedUserCount, new AtomicLong())
    }
  })


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(notifierConfig.dbHost, notifierConfig.dbPort, notifierConfig)
    val session = cassandraUtil.session

    session.execute(s"DROP KEYSPACE IF EXISTS ${notifierConfig.dbKeyspace}")
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
    // Clear the metrics

    BaseMetricsReporter.gaugeMetrics.clear()
    when(mockHttpUtil.get(ArgumentMatchers.contains("/private/user/v1/read/"), any[Map[String, String]]())).thenReturn(HTTPResponse(200, """{"id":"","ver":"private","ts":"2020-10-21 14:10:49:964+0000","params":{"resmsgid":null,"msgid":"a6f3e248-c504-4c2f-9bfa-90f54abd2e30","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"firstName":"test12","lastName":"A","maskedPhone":"******0183","rootOrgName":"ORG_002","userName":"teast123","rootOrgId":"01246944855007232011"}}}"""))
    when(mockHttpUtil.post(endsWith("/v2/notification"), any[String], any[Map[String, String]]())).thenReturn(HTTPResponse(200, """{"id":"api.notification","ver":"v2","ts":"2020-10-21 14:12:09:065+0000","params":{"resmsgid":null,"msgid":"0df38787-1168-4ae0-aa4b-dcea23ea81e4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":"SUCCESS"}}"""))
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

  "NotifierFunction " should "should send notify user" in {
    implicit val notificationMetaTypeInfo: TypeInformation[NotificationMetaData] = TypeExtractor.getForClass(classOf[NotificationMetaData])
    new NotifierFunction(notifierConfig, mockHttpUtil,cassandraUtil).processElement(NotificationMetaData("userId", "Course Name", new Date(), "do_11309999837886054415", "0131000245281587206", "template_01_dev_001",0, 0), null, metrics)
    metrics.get(s"${notifierConfig.courseBatchdbReadCount}") should be(1)
    metrics.get(s"${notifierConfig.notifiedUserCount}") should be(1)
    metrics.get(s"${notifierConfig.skipNotifyUserCount}") should be(0)
  }
}

