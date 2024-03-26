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
import org.mockito.{ArgumentMatchers, Mockito}
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
  val server = new MockWebServer()

  var currentMilliSecond = 1605816926271L

  override protected def beforeAll(): Unit = {
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
    super.beforeAll()
  }

  "MetricsDataTransformerTaskTestSpec" should "generate event" in {
    server.start(9200)
    server.enqueue(new MockResponse().setHeader(
      "Content-Type", "application/json"
    ).setBody("""{"_index":"kp_audit_log_2018_7","_type":"ah","_id":"HLZ-1ngBtZ15DPx6ENjU","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1}"""))

    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic))//.thenReturn(new MetricsDataTransformerMapSource)
    implicit val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())

    new MetricsDataTransformerStreamTask(jobConfig, mockKafkaUtil, httpUtil).process()

    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
    server.close()
  }

}

class MetricsDataTransformerMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {
    // Event to be skipped
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.SKIP_EVENT), 0, 10))
  }

  override def cancel(): Unit = {}
}
