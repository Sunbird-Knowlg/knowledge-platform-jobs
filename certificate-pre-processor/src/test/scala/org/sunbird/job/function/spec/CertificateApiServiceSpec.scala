package org.sunbird.job.function.spec

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.ArgumentMatchers.{any, endsWith}
import org.mockito.Mockito.when
import org.sunbird.job.Metrics
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.functions.CertificateApiService
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.{HTTPResponse, HttpUtil}
import org.sunbird.spec.{BaseTestSpec}
import redis.embedded.RedisServer

import scala.collection.JavaConverters._

class CertificateApiServiceSpec extends BaseTestSpec {

  val mockHttpUtil = mock[HttpUtil]
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: CertificatePreProcessorConfig = new CertificatePreProcessorConfig(config)
  var redisServer: RedisServer = _
  val redisConnect = new RedisConnect(jobConfig)
  val metrics = Metrics(new ConcurrentHashMap[String, AtomicLong]() {
    {
      put(jobConfig.cacheReadCount, new AtomicLong())
    }
  })

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    redisServer = new RedisServer(6340)
    redisServer.start()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      redisServer.stop()
    } catch {
      case ex: Exception => {
      }
    }
  }


  it should "getUsersFromUserCriteria" in {
    val userCriteria: util.Map[String, AnyRef] = Map("status" -> 2.asInstanceOf[AnyRef]).asJava
    val userIds: List[String] = List("95e4942d-cbe8-477d-aebd-ad8e6de4bfc8", "d4b1ba7a-84fc-45be-a2b6-19ef08995282")
    CertificateApiService.httpUtil = mockHttpUtil
    when(mockHttpUtil.post(endsWith("/v1/search"), any[String])).thenReturn(HTTPResponse(200, """{"id": "api.user.search","ver": "v1","ts": "2020-10-24 14:01:25:831+0000","params": {"resmsgid": null,"msgid": "e783983e25c18b9c81e6a9be53dbbc95","err": null,"status": "success","errmsg": null },"responseCode":"OK","result":{"count":2,"content":[{"identifier":"95e4942d-cbe8-477d-aebd-ad8e6de4bfc8"},{"identifier":"d4b1ba7a-84fc-45be-a2b6-19ef08995282"}]}}""".stripMargin))
    val list = CertificateApiService.getUsersFromUserCriteria(userCriteria, userIds)(jobConfig)
    list.size should be(2)
    list should contain allOf("95e4942d-cbe8-477d-aebd-ad8e6de4bfc8", "d4b1ba7a-84fc-45be-a2b6-19ef08995282")
  }

  it should "throw error while userIds search" in intercept[Exception] {
    val userCriteria: util.Map[String, AnyRef] = Map("status" -> 2.asInstanceOf[AnyRef]).asJava
    val userIds: List[String] = List("95e4942d-cbe8-477d-aebd-ad8e6de4bfc8", "d4b1ba7a-84fc-45be-a2b6-19ef08995282")
    CertificateApiService.httpUtil = mockHttpUtil
    when(mockHttpUtil.post(endsWith("/v1/search"), any[String])).thenReturn(HTTPResponse(500, """{"id": "api.user.search","ver": "v1","ts": "2020-10-24 14:01:25:831+0000","params": {"resmsgid": null,"msgid": "e783983e25c18b9c81e6a9be53dbbc95","err": null,"status": "failed","errmsg": "server error" },"responseCode":"SERVER_ERROR","result": {"messages": null}}""".stripMargin))
    CertificateApiService.getUsersFromUserCriteria(userCriteria, userIds)(jobConfig)
  }

  it should "getUserDetails" in {
    val userId = "95e4942d-cbe8-477d-aebd-ad8e6de4bfc8"
    CertificateApiService.httpUtil = mockHttpUtil
    when(mockHttpUtil.post(endsWith("/v1/search"), any[String])).thenReturn(HTTPResponse(200, """{"id":"","ver":"private","ts":"2020-10-28 07:31:36:637+0000","params":{"resmsgid":null,"msgid":"8e27cbf5-e299-43b0-bca7-8347f7e5abcf","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"count":1,"content":[{"firstName":"Reviewer","lastName":"User","maskedPhone":"******7418","rootOrgName":"Sunbird","userName":"ntptest103","rootOrgId":"ORG_001"}]}}}""".stripMargin))
    val map = CertificateApiService.getUserDetails(userId)(jobConfig)
    map.asScala.keySet.map(c => c) should contain allOf("firstName", "lastName", "maskedPhone", "rootOrgName", "userName", "rootOrgId")
  }

  it should "throw error for invalid userId search" in intercept[Exception] {
    val userId = "95e4942d-cbe8-477d-aebd-ad8e6de4bfc8"
    CertificateApiService.httpUtil = mockHttpUtil
    when(mockHttpUtil.post(endsWith("/v1/search"), any[String])).thenReturn(HTTPResponse(500, """{"id":"api.user.search","ver":"v1","ts":"2020-10-24 14:24:57:555+0000","params":{"resmsgid":null,"msgid":"df3342f1082aa191128453838fb4e61f","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{ "count": 0,"content": []}}""".stripMargin))
    CertificateApiService.getUserDetails(userId)(jobConfig)
  }

  it should "throw error while userId search" in intercept[Exception] {
    val userId = "95e4942d-cbe8-477d-aebd-ad8e6de4bfc8"
    CertificateApiService.httpUtil = mockHttpUtil
    when(mockHttpUtil.post(endsWith("/v1/search"), any[String])).thenReturn(HTTPResponse(500, """{"id":"","ver":"private","ts":"2020-10-28 07:31:36:637+0000","params":{"resmsgid":null,"msgid":"8e27cbf5-e299-43b0-bca7-8347f7e5abcf","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"response":{"count":1,"content":[{"firstName":"Reviewer","lastName":"User","maskedPhone":"******7418","rootOrgName":"Sunbird","userName":"ntptest103","rootOrgId":"ORG_001"}]}}}""".stripMargin))
    CertificateApiService.getUserDetails(userId)(jobConfig)
  }

  it should "readOrgKeys" in {
    val rootOrgId = "ORG_001"
    CertificateApiService.httpUtil = mockHttpUtil
    when(mockHttpUtil.post(endsWith("/v1/org/read"), any[String])).thenReturn(HTTPResponse(200, """{ "id": "api.org.read", "ver": "v1", "ts": "2020-10-24 14:47:23:631+0000", "params": { "resmsgid": null, "msgid": "cc58e03e2789f6db8b4695a43a5c8a39", "err": null, "status": "success", "errmsg": null }, "responseCode": "OK", "result": {"keys": {"signKeys": [{"testKey": "testValue"}]}}}""".stripMargin))
    val map = CertificateApiService.readOrgKeys(rootOrgId)(jobConfig)
    map.asScala.keySet.map(c => c) should contain("id")
  }

  it should "readContent from cache" in {
    val courseId = "do_11312403964547072012070"
    val dataCache = new DataCache(jobConfig, redisConnect, 2, List("name", "status", "code"))
    dataCache.init()
    dataCache.set(courseId, """{"name": "Test-audit-svg-7-oct-2", "status": "Live", "code": "org.sunbird.4SZ9XP"}""")
    val map = CertificateApiService.readContent(courseId, dataCache)(jobConfig, metrics)
    dataCache.close()
    map.keySet should contain("name")
    metrics.get(s"${jobConfig.cacheReadCount}") should be(1)
    metrics.reset(jobConfig.cacheReadCount)
  }

  it should "readContent from content read api" in {
    val courseId = "do_11312403964547072012070"
    val dataCache = new DataCache(jobConfig, redisConnect, 2, List())
    dataCache.init()
    CertificateApiService.httpUtil = mockHttpUtil
    when(mockHttpUtil.get(endsWith("/v3/read/" + courseId))).thenReturn(HTTPResponse(200, """{ "id": "api.v3.read", "ver": "1.0", "ts": "2020-10-24T15:25:39.187Z", "params": { "resmsgid": "2be6e430-160d-11eb-98c2-3bbec8c9cf05", "msgid": "2be4e860-160d-11eb-98c2-3bbec8c9cf05", "status": "successful", "err": null, "errmsg": null }, "responseCode": "OK", "result": {"content": {"name": "Test-audit-svg-7-oct-2", "status": "Live", "code": "org.sunbird.4SZ9XP"}}}""".stripMargin))
    val map = CertificateApiService.readContent(courseId, dataCache)(jobConfig, metrics)
    dataCache.close()
    map.keySet should contain("name")
    metrics.reset(jobConfig.cacheReadCount)
  }

  it should "throw error for readContent" in intercept[Exception] {
    val courseId = "do_11312403964547072012070"
    val dataCache = new DataCache(jobConfig, redisConnect, 2, List())
    dataCache.init()
    CertificateApiService.httpUtil = mockHttpUtil
    when(mockHttpUtil.get(endsWith("/v3/read/" + courseId))).thenReturn(HTTPResponse(404, """{"id":"api.v3.read","ver":"1.0","ts":"2020-10-24T16:35:55.466Z","params":{"resmsgid":"fd0002a0-1616-11eb-98c2-3bbec8c9cf05","msgid":null,"status":"failed","err":"NOT_FOUND","errmsg":"Error! Node(s) doesn't Exists. | [Invalid Node Id.]: {{course-id-1}}"},"responseCode":"RESOURCE_NOT_FOUND","result":{"messages":null}}""".stripMargin))
    CertificateApiService.readContent(courseId, dataCache)(jobConfig, metrics)
  }

}
