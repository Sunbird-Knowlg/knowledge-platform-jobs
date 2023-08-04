package org.sunbird.job.spec

import java.util
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.qrimagegenerator.domain.Event
import org.sunbird.job.qrimagegenerator.task.{QRCodeImageGeneratorConfig, QRCodeImageGeneratorTask}
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, ElasticSearchUtil, JSONUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

class QRCodeImageGeneratorTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: QRCodeImageGeneratorConfig = new QRCodeImageGeneratorConfig(config)
  val cloudStorageUtil:CloudStorageUtil = new CloudStorageUtil(jobConfig)
  var cassandraUtils: CassandraUtil = _
  val mockElasticUtil: ElasticSearchUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())
  var currentMilliSecond = 1605816926271L

  override protected def beforeAll(): Unit = {
    BaseMetricsReporter.gaugeMetrics.clear()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtils = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort, jobConfig)
    val session = cassandraUtils.session
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
    flinkCluster.before()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    flinkCluster.after()
    super.afterAll()
  }

  "QRCodeImageGeneratorTask" should "generate event" in {

    val N3X6Y3Json = """{"identifier":"N3X6Y3", "filename":"2_N3X6Y3", "channel":"b00bc992ef25f1a9a8d63291e20efc8d"}"""
    val U3J1J9Json = """{"identifier":"U3J1J9", "filename":"0_U3J1J9", "channel":"b00bc992ef25f1a9a8d63291e20efc8d"}"""
    val V2B5A2Json = """{"identifier":"V2B5A2", "filename":"1_V2B5A2", "channel":"b00bc992ef25f1a9a8d63291e20efc8d"}"""
    val F6J3E7Json = """{"identifier":"F6J3E7", "filename":"0_F6J3E7", "channel":"b00bc992ef25f1a9a8d63291e20efc8d"}"""
    val Q1I5I3Json = """{"identifier":"Q1I5I3", "filename":"0_Q1I5I3", "channel":"b00bc992ef25f1a9a8d63291e20efc8d"}"""
    when(mockElasticUtil.getDocumentAsString("Q1I5I3")).thenReturn(Q1I5I3Json)
    when(mockElasticUtil.getDocumentAsString("N3X6Y3")).thenReturn(N3X6Y3Json)
    when(mockElasticUtil.getDocumentAsString("U3J1J9")).thenReturn(U3J1J9Json)
    when(mockElasticUtil.getDocumentAsString("V2B5A2")).thenReturn(V2B5A2Json)
    when(mockElasticUtil.getDocumentAsString("F6J3E7")).thenReturn(F6J3E7Json)

    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new QRCodeImageGeneratorMapSource)
    new QRCodeImageGeneratorTask(jobConfig, mockKafkaUtil).process()
//    assertThrows[JobExecutionException] {
//      new QRCodeImageGeneratorTask(jobConfig, mockKafkaUtil).process()
//    }
    
  }
}

class QRCodeImageGeneratorMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    // Valid event
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1), 0, 10))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2), 0, 11))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_3), 0, 12))
    // Invalid event
//    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_4)))
  }

  override def cancel(): Unit = {}
}