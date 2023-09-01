package org.sunbird.job.spec

import java.util
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.sunbird.job.util.JSONUtil
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.transaction.task.{AuditEventGeneratorConfig, AuditEventGeneratorStreamTask}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

class AuditEventGeneratorTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: AuditEventGeneratorConfig = new AuditEventGeneratorConfig(config)
  var currentMilliSecond = 1605816926271L

  override protected def beforeAll(): Unit = {
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    flinkCluster.after()
    super.afterAll()
  }

  "AuditEventGeneratorStreamTask" should "generate audit event" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AuditEventGeneratorMapSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaOutputTopic)).thenReturn(new AuditEventSink)

    new AuditEventGeneratorStreamTask(jobConfig, mockKafkaUtil).process()

    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(2)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(1)

    AuditEventSink.values.size() should be(1)
    AuditEventSink.values.forEach(event => {
      val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](event)
      eventMap("eid") should be("AUDIT")
      eventMap("ver") should be("3.0")
      eventMap("edata") shouldNot be(null)
    })
  }

  "AuditEventGeneratorStreamTask" should "increase metric for unknown schema" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new RandomObjectTypeAuditEventGeneratorMapSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaOutputTopic)).thenReturn(new AuditEventSink)

    new AuditEventGeneratorStreamTask(jobConfig, mockKafkaUtil).process()

    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.emptySchemaEventCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.emptyPropsEventCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(0)
  }
}

class AuditEventGeneratorMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1), 0, 10))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_5), 0, 11))
  }

  override def cancel(): Unit = {}
}

class RandomObjectTypeAuditEventGeneratorMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_8), 0, 10))
  }

  override def cancel(): Unit = {}
}

class AuditEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      AuditEventSink.values.add(value)
    }
  }
}

object AuditEventSink {
  val values: util.List[String] = new util.ArrayList()
}