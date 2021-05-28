package org.sunbird.job.function.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito.when
import org.sunbird.collectioncert.domain.Event
import org.sunbird.job.Metrics
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.cert.task.CollectionCertPreProcessorConfig
import org.sunbird.job.fixture.EvenFixture
import org.sunbird.job.functions.CollectionCertPreProcessorFn
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil, ScalaJsonUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

class CollectionCertPreProcessFnTestSpec extends BaseTestSpec {
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    var cassandraUtil: CassandraUtil = _
    val config: Config = ConfigFactory.load("test.conf")
    lazy val jobConfig: CollectionCertPreProcessorConfig = new CollectionCertPreProcessorConfig(config)
    val httpUtil: HttpUtil = new HttpUtil
    val mockHttpUtil:HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    val metricJson = s"""{"${jobConfig.totalEventsCount}": 0, "${jobConfig.skippedEventCount}": 0}"""
    val mockMetrics = mock[Metrics](Mockito.withSettings().serializable())
    var jedis: Jedis = _
    var cache: DataCache = _
    var redisServer: RedisServer = _
    redisServer = new RedisServer(6340)
    redisServer.start()

    override protected def beforeAll(): Unit = {
        super.beforeAll()
        val redisConnect = new RedisConnect(jobConfig)
        jedis = redisConnect.getConnection(jobConfig.collectionCacheStore)
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
        cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
        val session = cassandraUtil.session
        val dataLoader = new CQLDataLoader(session)
        dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
        cache = new DataCache(jobConfig, redisConnect, jobConfig.collectionCacheStore, null)
        cache.init()
        jedis.flushDB()
    }

    override protected def afterAll(): Unit = {
        super.afterAll()
        try {
            EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
        } catch {
            case ex: Exception => ex.printStackTrace()
        }
        redisServer.stop()
    }
    
    "CertPreProcess " should " issue certificate to valid request" in {
        val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EvenFixture.EVENT_1), 0, 0)
        val template = ScalaJsonUtil.deserialize[Map[String, String]](EvenFixture.TEMPLATE_1)
        when(mockHttpUtil.get(ArgumentMatchers.contains(jobConfig.userReadApi), ArgumentMatchers.any[Map[String, String]]())).thenReturn(HTTPResponse(200, EvenFixture.USER_1))
        when(mockHttpUtil.get(ArgumentMatchers.contains(jobConfig.contentReadApi), ArgumentMatchers.any[Map[String, String]]())).thenReturn(HTTPResponse(200, EvenFixture.CONTENT_1))
        val certEvent = new CollectionCertPreProcessorFn(jobConfig, mockHttpUtil)(stringTypeInfo, cassandraUtil).issueCertificate(event, template)(cassandraUtil, cache, mockMetrics, jobConfig, mockHttpUtil)
        certEvent shouldNot be(null)        
    }

}
