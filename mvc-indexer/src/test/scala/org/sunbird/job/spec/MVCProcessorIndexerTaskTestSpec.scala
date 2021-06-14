package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import okhttp3.mockwebserver.{MockResponse, MockWebServer}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.sunbird.job.util.{ElasticSearchUtil, HttpUtil, JSONUtil}
import org.mockito.Mockito
import org.mockito.Mockito.{doNothing, when}
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.mvcindexer.task.{MVCIndexerConfig, MVCIndexerStreamTask}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

import java.util

class MVCProcessorIndexerTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: MVCIndexerConfig = new MVCIndexerConfig(config)
  val esUtil: ElasticSearchUtil = null
  val httpUtil: HttpUtil = null
  val server = new MockWebServer()

  var currentMilliSecond = 1605816926271L

  override protected def beforeAll(): Unit = {
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
    super.beforeAll()
  }

  "MVCProcessorIndexerStreamTask" should "generate event" in {
    server.start(9200)
    server.enqueue(new MockResponse().setHeader(
      "Content-Type", "application/json"
    ).setBody("""{"_index":"kp_audit_log_2018_7","_type":"ah","_id":"HLZ-1ngBtZ15DPx6ENjU","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1}"""))

    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new MVCProcessorIndexerMapSource)

    new MVCIndexerStreamTask(jobConfig, mockKafkaUtil, esUtil, httpUtil).process()

    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(2)
    server.close()
  }

  "MVCProcessorIndexerStreamTask" should "throw exception and increase es error count" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new MVCProcessorIndexerMapSource)

    try {
      new MVCIndexerStreamTask(jobConfig, mockKafkaUtil, esUtil, httpUtil).process()
    } catch {
      case ex: JobExecutionException =>
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.esFailedEventCount}").getValue() should be(1)
    }
  }
}

class MVCProcessorIndexerMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {
    // Valid event
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1), 0, 10))
    // Invalid event
//    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_4), 0, 11))
  }

  override def cancel(): Unit = {}
}