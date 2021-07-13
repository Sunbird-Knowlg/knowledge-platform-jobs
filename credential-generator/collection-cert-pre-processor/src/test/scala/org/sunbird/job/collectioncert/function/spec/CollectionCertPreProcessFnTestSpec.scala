package org.sunbird.job.collectioncert.function.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito.when
import org.mockito.{ArgumentMatchers, Mockito}
import org.sunbird.job.Metrics
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.collectioncert.domain.Event
import org.sunbird.job.collectioncert.fixture.EventFixture
import org.sunbird.job.collectioncert.functions.CollectionCertPreProcessorFn
import org.sunbird.job.collectioncert.task.CollectionCertPreProcessorConfig
import org.sunbird.job.util._
import org.sunbird.spec.BaseTestSpec
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
        val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventFixture.EVENT_1), 0, 0)
        val template = ScalaJsonUtil.deserialize[Map[String, String]](EventFixture.TEMPLATE_1)
        when(mockHttpUtil.get(ArgumentMatchers.contains(jobConfig.userReadApi), ArgumentMatchers.any[Map[String, String]]())).thenReturn(HTTPResponse(200, EventFixture.USER_1))
        when(mockHttpUtil.get(ArgumentMatchers.contains(jobConfig.contentReadApi), ArgumentMatchers.any[Map[String, String]]())).thenReturn(HTTPResponse(200, EventFixture.CONTENT_1))
        val certEvent = new CollectionCertPreProcessorFn(jobConfig, mockHttpUtil)(stringTypeInfo, cassandraUtil).issueCertificate(event, template)(cassandraUtil, cache, mockMetrics, jobConfig, mockHttpUtil)
        certEvent shouldNot be(null)        
    }


    "CertPreProcess When User Last Name is Empty " should " Should get Valid getRecipientName " in {
        val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventFixture.EVENT_1), 0, 0)
        val template = ScalaJsonUtil.deserialize[Map[String, String]](EventFixture.TEMPLATE_1)
        when(mockHttpUtil.get(ArgumentMatchers.contains(jobConfig.userReadApi), ArgumentMatchers.any[Map[String, String]]())).thenReturn(HTTPResponse(200, EventFixture.USER_2_EMPTY_LASTNAME))
        when(mockHttpUtil.get(ArgumentMatchers.contains(jobConfig.contentReadApi), ArgumentMatchers.any[Map[String, String]]())).thenReturn(HTTPResponse(200, EventFixture.CONTENT_1))
        val certEvent: String = new CollectionCertPreProcessorFn(jobConfig, mockHttpUtil)(stringTypeInfo, cassandraUtil).issueCertificate(event, template)(cassandraUtil, cache, mockMetrics, jobConfig, mockHttpUtil)
        certEvent shouldNot be(null)
        getRecipientName(certEvent) should be("Rajesh")
    }


    "CertPreProcess When User Last Name is Null " should " Should get Valid getRecipientName " in {
        val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventFixture.EVENT_1), 0, 0)
        val template = ScalaJsonUtil.deserialize[Map[String, String]](EventFixture.TEMPLATE_1)
        when(mockHttpUtil.get(ArgumentMatchers.contains(jobConfig.userReadApi), ArgumentMatchers.any[Map[String, String]]())).thenReturn(HTTPResponse(200, EventFixture.USER_3_NULL_VALUE_LASTNAME))
        when(mockHttpUtil.get(ArgumentMatchers.contains(jobConfig.contentReadApi), ArgumentMatchers.any[Map[String, String]]())).thenReturn(HTTPResponse(200, EventFixture.CONTENT_1))
        val certEvent: String = new CollectionCertPreProcessorFn(jobConfig, mockHttpUtil)(stringTypeInfo, cassandraUtil).issueCertificate(event, template)(cassandraUtil, cache, mockMetrics, jobConfig, mockHttpUtil)
        certEvent shouldNot be(null)
        getRecipientName(certEvent) should be("Suresh")
    }


    def getRecipientName(certEvent: String): String = {
        import java.util
        import com.google.gson.Gson
        import com.google.gson.internal.LinkedTreeMap
        val gson = new Gson()
        val certMapEvent = gson.fromJson(certEvent, new java.util.LinkedHashMap[String, Any]().getClass)
        val certEdata = certMapEvent.getOrDefault("edata", new util.LinkedHashMap[String, Any]()).asInstanceOf[LinkedTreeMap[String, Any]]
        val certUserData = certEdata.getOrDefault("data", new util.LinkedList[util.LinkedHashMap[String, Any]]()).asInstanceOf[util.ArrayList[LinkedTreeMap[String, Any]]]
        val recipiendName = certUserData.get(0).get("recipientName").asInstanceOf[String]
        println("recipiendName" + recipiendName)
        recipiendName
    }

}
