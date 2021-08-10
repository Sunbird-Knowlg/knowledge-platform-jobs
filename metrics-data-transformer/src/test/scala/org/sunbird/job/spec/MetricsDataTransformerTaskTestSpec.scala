package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import okhttp3.mockwebserver.{MockResponse, MockWebServer}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.metricstransformer.domain.Event
import org.sunbird.job.metricstransformer.task.{MetricsDataTransformerConfig, MetricsDataTransformerStreamTask}
import org.sunbird.job.util.{HTTPResponse, HttpUtil, JSONUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

import java.util

class MetricsDataTransformerTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: MetricsDataTransformerConfig = new MetricsDataTransformerConfig(config)
  val httpUtil: HttpUtil = new HttpUtil()

  var currentMilliSecond = 1605816926271L

  override protected def beforeAll(): Unit = {
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
    super.beforeAll()
  }

  "MetricsDataTransformerTaskTestSpec" should "generate event" in {

    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new MetricsDataTransformerMapSource)
    implicit val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(200, """{"id":"api.v3.read.get","ver":"1.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"content":{"parent":"do_123","origin":"do_119831368202241","mediaType":"content","name":"APSWREIS-TGT2-Day5","identifier":"do_234","description":"Enter description for Collection","resourceType":"Collection","mimeType":"application/vnd.ekstep.content-collection","contentType":"Collection","language":["English"],"objectType":"Content","status":"Live","idealScreenSize":"normal","contentEncoding":"gzip","osId":"org.ekstep.quiz.app","contentDisposition":"inline","childNodes":["do_345"],"visibility":"Default","pkgVersion":2,"idealScreenDensity":"hdpi"}}}"""))
    when(mockHttpUtil.patch(anyString(), any(), any())).thenReturn(HTTPResponse(200, """{"id":"api.content.hierarchy.get","ver":"3.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"content":{"rootId":"do_123"}}}"""))

    new MetricsDataTransformerStreamTask(jobConfig, mockKafkaUtil, mockHttpUtil).process()

    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
  }

}

class MetricsDataTransformerMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {
    // Valid event
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1), 0, 10))
  }

  override def cancel(): Unit = {}
}
