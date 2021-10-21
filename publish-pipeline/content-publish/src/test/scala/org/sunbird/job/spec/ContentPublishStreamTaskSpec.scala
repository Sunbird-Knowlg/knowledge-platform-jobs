package org.sunbird.job.spec

import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, Neo4JUtil}

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
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.content.publish.domain.Event
import org.sunbird.job.content.task.{ContentPublishConfig, ContentPublishStreamTask}
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import java.util

class ContentPublishStreamTaskSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val strTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val jobConfig: ContentPublishConfig = new ContentPublishConfig(config)

  //  val mockHttpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  val mockHttpUtil: HttpUtil = new HttpUtil
  val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  var cassandraUtil: CassandraUtil = _
  val publishConfig: PublishConfig = new PublishConfig(config, "")
  val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(publishConfig)
  var jedis: Jedis = _
  val mockDataCache: DataCache = mock[DataCache](Mockito.withSettings().serializable())
  var redisServer: RedisServer = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    redisServer = new RedisServer(6340)
    redisServer.start()
    val redisConnect = new RedisConnect(jobConfig)
    jedis = redisConnect.getConnection(jobConfig.nodeStore)
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
    flinkCluster.before()
    jedis.flushDB()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception =>
    }
    flinkCluster.after()
    redisServer.stop()
  }

  def initialize(): Unit = {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new ContentPublishEventSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.postPublishTopic)).thenReturn(new PostPublishEventSink)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaErrorTopic)).thenReturn(new ContentPublishFailedEventSink)
  }

  "ContentPublishStreamTask process event " should " publish the content " in {
    when(mockNeo4JUtil.getNodeProperties(anyString())).thenReturn(new util.HashMap[String, AnyRef])
    initialize()
    new ContentPublishStreamTask(jobConfig, mockKafkaUtil, mockHttpUtil, mockNeo4JUtil).process()
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.contentPublishEventCount}").getValue() should be(1)
  }
}

private class ContentPublishEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(jsonToEvent(EventFixture.PDF_EVENT1))
  }

  override def cancel(): Unit = {}

  def jsonToEvent(json: String): Event = {
    val gson = new Gson()
    val data = gson.fromJson(json, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]]
    val metadataMap = data.get("edata").asInstanceOf[util.Map[String, Any]].get("metadata").asInstanceOf[util.Map[String, Any]]
    metadataMap.put("pkgVersion", metadataMap.get("pkgVersion").asInstanceOf[Double].toInt)
    new Event(data, 0, 10)
  }
}

class ContentPublishFailedEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      ContentPublishFailedEventSink.values.add(value)
    }
  }
}

object ContentPublishFailedEventSink {
  val values: util.List[String] = new util.ArrayList()
}

class PostPublishEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      PostPublishEventSink.values.add(value)
    }
  }
}

object PostPublishEventSink {
  val values: util.List[String] = new util.ArrayList()
}
