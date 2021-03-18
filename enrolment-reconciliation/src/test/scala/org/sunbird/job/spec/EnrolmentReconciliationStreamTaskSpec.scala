package org.sunbird.job.spec

import com.google.gson.Gson
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
import org.mockito.Mockito._
import org.sunbird.job.cache.RedisConnect
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.task.{EnrolmentReconciliationConfig, EnrolmentReconciliationStreamTask}
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer
import java.util

import scala.collection.JavaConverters._

class EnrolmentReconciliationStreamTaskSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  var redisServer: RedisServer = _
  redisServer = new RedisServer(6340)
  redisServer.start()
  var jedis: Jedis = _

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val gson = new Gson()
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: EnrolmentReconciliationConfig = new EnrolmentReconciliationConfig(config)


  var cassandraUtil: CassandraUtil = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val redisConnect = new RedisConnect(jobConfig)
    jedis = redisConnect.getConnection(jobConfig.nodeStore)
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
    val session = cassandraUtil.session

    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
    // Clear the metrics
    BaseMetricsReporter.gaugeMetrics.clear()
    jedis.flushDB()
    flinkCluster.before()

    updateRedis(jedis, EventFixture.CASE_1.asInstanceOf[Map[String, AnyRef]])
    updateRedis(jedis, EventFixture.CASE_2.asInstanceOf[Map[String, AnyRef]])
    updateRedis(jedis, EventFixture.CASE_3.asInstanceOf[Map[String, AnyRef]])
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
      redisServer.stop()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    flinkCluster.after()
  }

  def initialize() {
    when(mockKafkaUtil.kafkaMapSource(jobConfig.kafkaInputTopic)).thenReturn(new EnrolmentReconciliationMapSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaAuditEventTopic)).thenReturn(new AuditEventSink)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaFailedEventTopic)).thenReturn(new FailedEventSink)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaCertIssueTopic)).thenReturn(new CertificateIssuedEventsSink)
  }


  "EnrolmentReConciliation " should "validate metrics" in {
    initialize()
    new EnrolmentReconciliationStreamTask(jobConfig, mockKafkaUtil, httpUtil = new HttpUtil).process()
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventCount}").getValue() should be(2)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.dbUpdateCount}").getValue() should be(2)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skipEventsCount}").getValue() should be(1)
  }

  def updateRedis(jedis: Jedis, testData: Map[String, AnyRef]) {
    testData.get("cacheData").map(data => {
      data.asInstanceOf[List[Map[String, AnyRef]]].map(cacheData => {
        cacheData.map(x => {
          x._2.asInstanceOf[List[String]].foreach(d => {
            jedis.sadd(x._1, d)
          })
        })
      })
    })
  }

}

class EnrolmentReconciliationMapSource extends SourceFunction[java.util.Map[String, AnyRef]] {
  override def run(ctx: SourceContext[util.Map[String, AnyRef]]): Unit = {
    val gson = new Gson()
    val eventMap1 = gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala
    val eventMap2 = gson.fromJson(EventFixture.EVENT_2, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala
    ctx.collect(eventMap1.asJava)
    ctx.collect(eventMap2.asJava)
  }

  override def cancel(): Unit = {}
  
}

