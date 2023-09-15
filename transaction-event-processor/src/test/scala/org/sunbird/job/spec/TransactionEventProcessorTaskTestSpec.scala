package org.sunbird.job.spec

import java.util
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import okhttp3.mockwebserver.{MockResponse, MockWebServer}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.sunbird.job.util.{ElasticSearchUtil, JSONUtil}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.transaction.domain.Event
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.transaction.task.{TransactionEventProcessorConfig, TransactionEventProcessorStreamTask}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

class TransactionEventProcessorTaskTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: TransactionEventProcessorConfig = new TransactionEventProcessorConfig(config)
  val esUtil:ElasticSearchUtil = null
  val server = new MockWebServer()

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

//  "TransactionEventProcessorStreamTask" should "generate audit event" in {
//    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AuditEventMapSource)
//    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaOutputTopic)).thenReturn(new AuditEventSink)
//    if (jobConfig.auditEventGenerator) {
//      new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil, esUtil).process()
//
//      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(2)
//      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(0)
//      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(1)
//      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(0)
//
//      AuditEventSink.values.size() should be(1)
//      AuditEventSink.values.forEach(event => {
//        val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](event)
//        eventMap("eid") should be("AUDIT")
//        eventMap("ver") should be("3.0")
//        eventMap("edata") shouldNot be(null)
//      })
//    }
//  }

  "TransactionEventProcessorStreamTask" should "generate audit event" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AuditEventMapSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaObsrvOutputTopic)).thenReturn(new AuditEventSink)
    if (jobConfig.auditEventGenerator) {
            new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil, esUtil).process()

            BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(2)
            BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(0)
            BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(1)
            BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(0)

            AuditEventSink.values.size() should be(1)
            AuditEventSink.values.forEach(event => {
              val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](event)
              eventMap("eid") should be("AUDIT")
              eventMap("ver") should be("3.0")
              eventMap("edata") shouldNot be(null)
            })
          }
  }

 "TransactionEventProcessorStreamTask" should "not generate audit event" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AuditEventMapSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaOutputTopic)).thenReturn(new AuditEventSink)

    val setBoolean = config.withValue("job.audit-event-generator", ConfigValueFactory.fromAnyRef(false))
    val newConfig: TransactionEventProcessorConfig = new TransactionEventProcessorConfig(setBoolean)
    if (newConfig.auditEventGenerator) {

      new TransactionEventProcessorStreamTask(newConfig, mockKafkaUtil, esUtil).process()

      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.totalEventsCount}").getValue() should be(2)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.skippedEventCount}").getValue() should be(2)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.successEventCount}").getValue() should be(0)

    }
  }

  "TransactionEventProcessorStreamTask" should "increase metric for unknown schema" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new RandomObjectTypeAuditEventGeneratorMapSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaOutputTopic)).thenReturn(new AuditEventSink)
    if (jobConfig.auditEventGenerator) {

      new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil, esUtil).process()

      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.emptySchemaEventCount}").getValue() should be(1)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.emptyPropsEventCount}").getValue() should be(1)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(0)
    }
  }


    "TransactionEventProcessorStreamTask" should "not generate audit history indexer event" in {
      server.start(9200)
      server.enqueue(new MockResponse().setHeader(
        "Content-Type", "application/json"
      ).setBody("""{"_index":"kp_audit_log_2018_7","_type":"ah","_id":"HLZ-1ngBtZ15DPx6ENjU","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1}"""))

      when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AuditHistoryMapSource)
      if (jobConfig.auditHistoryIndexer) {
        new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil, esUtil).process()

        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(2)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(0)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(1)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.esFailedEventCount}").getValue() should be(1)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(0)
      }
    }

  "TransactionEventProcessorStreamTask" should "generate audit history indexer event" in {
    server.enqueue(new MockResponse().setHeader(
      "Content-Type", "application/json"
    ).setBody("""{"_index":"kp_audit_log_2018_7","_type":"ah","_id":"HLZ-1ngBtZ15DPx6ENjU","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1}"""))

    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AuditHistoryMapSource)
    val setBoolean = config.withValue("job.audit-history-indexer", ConfigValueFactory.fromAnyRef(true))
    val newConfig: TransactionEventProcessorConfig = new TransactionEventProcessorConfig(setBoolean)
    if (newConfig.auditHistoryIndexer) {
      new TransactionEventProcessorStreamTask(newConfig, mockKafkaUtil, esUtil).process()
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.totalEventsCount}").getValue() should be(2)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.successEventCount}").getValue() should be(2)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.failedEventCount}").getValue() should be(0)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.esFailedEventCount}").getValue() should be(0)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.skippedEventCount}").getValue() should be(0)
      server.close()
    }
  }

  "TransactionEventProcessorStreamTask" should "throw exception and increase es error count" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AuditHistoryMapSource)

      try {
        new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil,esUtil).process()
      } catch {
        case ex: JobExecutionException =>
          BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
          BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(0)
          BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(0)
          BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.esFailedEventCount}").getValue() should be(1)
          BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(0)
      }
    }
}

class AuditEventMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1), 0, 10))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_5), 0, 11))
}

  override def cancel(): Unit = {}
}

class AuditHistoryMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {

    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_9), 0, 10))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_12),0,11))
  }

  override def cancel(): Unit = {}
}

class EventMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {

    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1), 0, 10))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_9),0,11))
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