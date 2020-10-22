package org.sunbird.job.spec

import java.util

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.{any, endsWith}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.task.{PostCertificateProcessorConfig, PostCertificateProcessorStreamTask}
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

import scala.collection.JavaConverters._

class PostCertificateProcessFunctionTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val gson = new Gson()
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: PostCertificateProcessorConfig = new PostCertificateProcessorConfig(config)
  val mockHttpUtil: HttpUtil = mock[HttpUtil]


  var cassandraUtil: CassandraUtil = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
    val session = cassandraUtil.session

    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    // Clear the metrics
    testCassandraUtil(cassandraUtil)
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
    when(mockHttpUtil.post(endsWith("/private/user/v1/search"), any[String])).thenReturn(HTTPResponse(200, """{"id":"","ver":"private","ts":"2020-10-21 14:10:49:964+0000","params":{"resmsgid":null,"msgid":"a6f3e248-c504-4c2f-9bfa-90f54abd2e30","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"count":1,"content":[{"firstName":"test12","lastName":"A","maskedPhone":"******0183","rootOrgName":"ORG_002","userName":"teast123","rootOrgId":"01246944855007232011"}]}}}"""))
    when(mockHttpUtil.post(endsWith("/v2/notification"), any[String])).thenReturn(HTTPResponse(200, """{"id":"api.notification","ver":"v2","ts":"2020-10-21 14:12:09:065+0000","params":{"resmsgid":null,"msgid":"0df38787-1168-4ae0-aa4b-dcea23ea81e4","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":"SUCCESS"}}"""))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => {
      }
    }
    flinkCluster.after()
  }


  "PostCertificateProcess" should "update issued-certificates and notify user" in {
    PostCertificateProcessorStreamTask.httpUtil = mockHttpUtil
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaFailedEventTopic)).thenReturn(new failedEventSink)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaAuditEventTopic)).thenReturn(new auditEventSink)
    when(mockKafkaUtil.kafkaMapSource(jobConfig.kafkaInputTopic)).thenReturn(new PostCertificateProcessMapSource)
    new PostCertificateProcessorStreamTask(jobConfig, mockKafkaUtil).process()
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(2)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(2)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(0)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(0)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.dbReadCount}").getValue() should be(4)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.dbUpdateCount}").getValue() should be(2)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.notifiedUserCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skipNotifyUserCount}").getValue() should be(1)
    failedEventSink.values.size() should be(0)
    auditEventSink.values.size() should be(2)
  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }

}

class PostCertificateProcessMapSource extends SourceFunction[java.util.Map[String, AnyRef]] {
  override def run(ctx: SourceContext[util.Map[String, AnyRef]]): Unit = {
    val gson = new Gson()
    val eventMap1 = gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala
    val eventMap2 = gson.fromJson(EventFixture.EVENT_2, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala
    ctx.collect(eventMap1.asJava)
    ctx.collect(eventMap2.asJava)
  }

  override def cancel() = {}
}

class failedEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      failedEventSink.values.add(value)
    }
  }
}

object failedEventSink {
  val values: util.List[String] = new util.ArrayList()
}

class auditEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      auditEventSink.values.add(value)
    }
  }
}

object auditEventSink {
  val values: util.List[String] = new util.ArrayList()
}