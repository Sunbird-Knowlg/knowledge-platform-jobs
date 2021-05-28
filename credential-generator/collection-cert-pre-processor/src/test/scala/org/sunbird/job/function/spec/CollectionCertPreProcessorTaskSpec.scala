package org.sunbird.job.function.spec

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
import org.mockito.Mockito.when
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.DoNotDiscover
import org.sunbird.collectioncert.domain.Event
import org.sunbird.job.cache.RedisConnect
import org.sunbird.job.cert.task.{CollectionCertPreProcessorConfig, CollectionCertPreProcessorTask}
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EvenFixture
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

@DoNotDiscover
class CollectionCertPreProcessorTaskSpec extends BaseTestSpec {
    implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

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
    implicit val mockHttpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    val gson = new Gson()
    val config: Config = ConfigFactory.load("test.conf")
    val jobConfig: CollectionCertPreProcessorConfig = new CollectionCertPreProcessorConfig(config)


    var cassandraUtil: CassandraUtil = _

    override protected def beforeAll(): Unit = {
        super.beforeAll()
        val redisConnect = new RedisConnect(jobConfig)
        jedis = redisConnect.getConnection(jobConfig.collectionCacheStore)
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
        cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
        val session = cassandraUtil.session

        val dataLoader = new CQLDataLoader(session)
        dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
        // Clear the metrics
        BaseMetricsReporter.gaugeMetrics.clear()
        jedis.flushDB()
        flinkCluster.before()
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
        when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic))
          .thenReturn(new CollectionCertPreProcessorEventSource)
        when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaFailedEventTopic)).thenReturn(new FailedEventSink)
        when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaOutputTopic)).thenReturn(new GenerateCertificateSink)

        when(mockHttpUtil.get(ArgumentMatchers.contains(jobConfig.userReadApi), ArgumentMatchers.any[Map[String, String]]())).thenReturn(HTTPResponse(200, EvenFixture.USER_1))
        when(mockHttpUtil.get(ArgumentMatchers.contains(jobConfig.contentReadApi), ArgumentMatchers.any[Map[String, String]]())).thenReturn(HTTPResponse(200, EvenFixture.CONTENT_1))
    }
    "CollectionCertPreProcessor " should "validate metrics " in {
        initialize()
        new CollectionCertPreProcessorTask(jobConfig, mockKafkaUtil, mockHttpUtil).process()
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.dbReadCount}").getValue() should be(2)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(1)
    }

}

class CollectionCertPreProcessorEventSource extends SourceFunction[Event] {
    override def run(ctx: SourceContext[Event]): Unit = {
        ctx.collect(new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EvenFixture.EVENT_1), 0, 0))
    }
    override def cancel(): Unit = {}
}

class FailedEventSink extends SinkFunction[String] {

    override def invoke(value: String): Unit = {
        synchronized {
            FailedEventSink.values.add(value)
        }
    }
}

object FailedEventSink {
    val values: util.List[String] = new util.ArrayList()
}

class GenerateCertificateSink extends SinkFunction[String] {
    override def invoke(value: String): Unit = {
        synchronized {
            GenerateCertificateSink.values.add(value)
        }
    }
}

object GenerateCertificateSink {
    val values: util.List[String] = new util.ArrayList()
}

