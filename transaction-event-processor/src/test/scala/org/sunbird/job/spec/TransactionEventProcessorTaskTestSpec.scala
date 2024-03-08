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
import org.mockito.ArgumentMatchers.any
import org.sunbird.job.util.{ElasticSearchUtil, JSONUtil}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.exception.InvalidEventException
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
  val esUtil: ElasticSearchUtil = null
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

  "TransactionEventProcessorStreamTask" should "handle invalid events and increase metric count" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new failedEventMapSource)
    try {
      new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil, esUtil).process()
    } catch {
      case ex: JobExecutionException =>
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(0)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(1)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(0)
        throw new InvalidEventException(any[String])
    }
  }

  "TransactionEventProcessorStreamTask" should "skip events and increase metric count" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new skippedEventMapSource)
    try {
      new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil, esUtil).process()
    } catch {
      case ex: JobExecutionException =>
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(0)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(0)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(1)
        throw new InvalidEventException(any[String])
    }
  }

  "TransactionEventProcessorStreamTask" should "generate audit event" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AuditEventMapSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaAuditOutputTopic)).thenReturn(new AuditEventSink)
    val setBoolean = config.withValue("job.audit-event-generator", ConfigValueFactory.fromAnyRef(true))
    val newConfig: TransactionEventProcessorConfig = new TransactionEventProcessorConfig(setBoolean)
    if (newConfig.auditEventGenerator) {
      new TransactionEventProcessorStreamTask(newConfig, mockKafkaUtil, esUtil).process()

      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.totalAuditEventsCount}").getValue() should be(2)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.auditEventSuccessCount}").getValue() should be(1)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.failedAuditEventsCount}").getValue() should be(0)

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
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaAuditOutputTopic)).thenReturn(new AuditEventSink)

    if (jobConfig.auditEventGenerator) {

      new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil, esUtil).process()

      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalAuditEventsCount}").getValue() should be(2)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.auditEventSuccessCount}").getValue() should be(0)

    }
  }

  "TransactionEventProcessorStreamTask" should "increase metric for unknown schema" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new RandomObjectTypeAuditEventGeneratorMapSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaAuditOutputTopic)).thenReturn(new AuditEventSink)
    if (jobConfig.auditEventGenerator) {

      new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil, esUtil).process()

      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalAuditEventsCount}").getValue() should be(1)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.emptySchemaEventCount}").getValue() should be(1)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.emptyPropsEventCount}").getValue() should be(1)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.auditEventSuccessCount}").getValue() should be(0)
    }
  }

  "TransactionEventProcessorStreamTask" should "not generate audit history indexer event" in {
    server.start(9200)
    server.enqueue(new MockResponse().setHeader("X-Elastic-Product", "Elasticsearch").setHeader(
      "Content-Type", "application/json"
    ).setBody(
      """{"name": "MacBook-Air.local","cluster_name": "elasticsearch","cluster_uuid": "9ra4wTGZSamseO3I99w","version": {"number": "7.17.13","build_flavor": "default","build_type": "tar","build_hash": "2b211dbb8bfd7f5b44d356bdfe54b1050c13","build_date": "2023-08-31T17:33:19.958690787Z","build_snapshot": false,"lucene_version": "8.11.1","minimum_wire_compatibility_version": "6.8.0","minimum_index_compatibility_version": "6.0.0-beta1"},"tagline": "You Know, for Search"}
        |,{"_index":"kp_audit_log_2018_7","_type":"_doc","_id":"HLZ-1ngBtZ15DPx6ENjU","_version":1,"result":"created","_shards":{"total":2,"successful":0,"failed":1},"_seq_no":1,"_primary_term":1}""".stripMargin))

    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AuditHistoryMapSource)
    if (jobConfig.auditHistoryIndexer) {
      new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil, esUtil).process()

      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalAuditHistoryEventsCount}").getValue() should be(2)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.auditHistoryEventSuccessCount}").getValue() should be(0)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedAuditHistoryEventsCount}").getValue() should be(1)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.esFailedEventCount}").getValue() should be(1)
    }
  }

  "TransactionEventProcessorStreamTask" should "generate audit history indexer event" in {
    server.enqueue(new MockResponse().setHeader("X-Elastic-Product", "Elasticsearch").setHeader(
      "Content-Type", "application/json"
    ).setBody(
      """{"name": "MacBook-Air.local","cluster_name": "elasticsearch","cluster_uuid": "9ra4wTGZEFPeO3I99w","version": {"number": "7.17.13","build_flavor": "default","build_type": "tar","build_hash": "2b211dbb8bf7f5b44d356bdfe54b1050c13","build_date": "2023-08-31T17:33:19.958690787Z","build_snapshot": false,"lucene_version": "8.11.1","minimum_wire_compatibility_version": "6.8.0","minimum_index_compatibility_version": "6.0.0-beta1"},"tagline": "You Know, for Search"}
        |,{"_index":"kp_audit_log_2018_7","_type":"_doc","_id":"HLZ-1ngBtZ15DPx6ENjU","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1}""".stripMargin))

    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AuditHistoryMapSource)
    val setBoolean = config.withValue("job.audit-history-indexer", ConfigValueFactory.fromAnyRef(true))
    val newConfig: TransactionEventProcessorConfig = new TransactionEventProcessorConfig(setBoolean)
    if (newConfig.auditHistoryIndexer) {
      new TransactionEventProcessorStreamTask(newConfig, mockKafkaUtil, esUtil).process()
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.totalAuditHistoryEventsCount}").getValue() should be(2)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.auditHistoryEventSuccessCount}").getValue() should be(2)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.failedAuditHistoryEventsCount}").getValue() should be(0)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.esFailedEventCount}").getValue() should be(0)
      server.close()
    }
  }

  "TransactionEventProcessorStreamTask" should "throw exception and increase es error count" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AuditHistoryMapSource)

    try {
      new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil, esUtil).process()
    } catch {
      case ex: JobExecutionException =>
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalAuditHistoryEventsCount}").getValue() should be(1)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.auditHistoryEventSuccessCount}").getValue() should be(0)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedAuditHistoryEventsCount}").getValue() should be(0)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.esFailedEventCount}").getValue() should be(1)
    }
  }

  "TransactionEventProcessorStreamTask" should "not generate obsrv event" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AuditEventMapSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaObsrvOutputTopic)).thenReturn(new AuditEventSink)
    if (jobConfig.obsrvMetadataGenerator) {
      new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil, esUtil).process()

      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalObsrvMetaDataGeneratorEventsCount}").getValue() should be(2)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.obsrvMetaDataGeneratorEventsSuccessCount}").getValue() should be(0)
    }
  }

  "TransactionEventProcessorStreamTask" should "generate obsrv event" in {
    val setBoolean = config.withValue("job.obsrv-metadata-generator", ConfigValueFactory.fromAnyRef(true))
    val newConfig: TransactionEventProcessorConfig = new TransactionEventProcessorConfig(setBoolean)

    when(mockKafkaUtil.kafkaJobRequestSource[Event](newConfig.kafkaInputTopic)).thenReturn(new EventMapSource)
    when(mockKafkaUtil.kafkaStringSink(newConfig.kafkaObsrvOutputTopic)).thenReturn(new AuditEventSink)

    if (newConfig.obsrvMetadataGenerator) {
      new TransactionEventProcessorStreamTask(newConfig, mockKafkaUtil, esUtil).process()

      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.totalObsrvMetaDataGeneratorEventsCount}").getValue() should be(2)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.obsrvMetaDataGeneratorEventsSuccessCount}").getValue() should be(2)
      BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.failedObsrvMetaDataGeneratorEventsCount}").getValue() should be(0)
    }
  }

  "TransactionEventProcessorStreamTask" should "increase metrics and throw exception for invalid event" in {
    val setBoolean = config.withValue("job.obsrv-metadata-generator", ConfigValueFactory.fromAnyRef(true))
    val newConfig: TransactionEventProcessorConfig = new TransactionEventProcessorConfig(setBoolean)

    when(mockKafkaUtil.kafkaJobRequestSource[Event](newConfig.kafkaInputTopic)).thenReturn(new EventMapSource)
    when(mockKafkaUtil.kafkaStringSink(newConfig.kafkaObsrvOutputTopic)).thenReturn(new AuditEventSink)

    try {
      new TransactionEventProcessorStreamTask(newConfig, mockKafkaUtil, esUtil).process()
    } catch {
      case ex: JobExecutionException =>
        BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.totalObsrvMetaDataGeneratorEventsCount}").getValue() should be(2)
        BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.obsrvMetaDataGeneratorEventsSuccessCount}").getValue() should be(0)
        BaseMetricsReporter.gaugeMetrics(s"${newConfig.jobName}.${newConfig.failedObsrvMetaDataGeneratorEventsCount}").getValue() should be(1)
    }
  }

  "TransactionEventProcessorStreamTask" should "throw exception in TransactionEventRouter" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new failedEventMapSource)

    try {
      new TransactionEventProcessorStreamTask(jobConfig, mockKafkaUtil, esUtil).process()
    } catch {
      case ex: JobExecutionException =>
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(0)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(1)
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
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_12), 0, 11))
  }

  override def cancel(): Unit = {}
}

class EventMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {

    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2), 0, 10))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_3), 0, 10))
  }

  override def cancel(): Unit = {}
}

class failedEventMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {

    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_14), 0, 10))
  }

  override def cancel(): Unit = {}
}

class skippedEventMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {

    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_13), 0, 10))
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