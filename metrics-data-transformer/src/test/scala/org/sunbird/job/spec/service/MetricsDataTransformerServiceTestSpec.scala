package org.sunbird.job.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.client.JobExecutionException
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.util.{ElasticSearchUtil, HTTPResponse, HttpUtil, JSONUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}
import org.sunbird.job.metricstransformer.domain.Event
import org.sunbird.job.metricstransformer.function.MetricsDataTransformerFunction
import org.sunbird.job.metricstransformer.task.MetricsDataTransformerConfig

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import org.sunbird.job.Metrics

class MetricsDataTransformerServiceTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: MetricsDataTransformerConfig = new MetricsDataTransformerConfig(config)
  val mockElasticUtil:ElasticSearchUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())
  lazy val metricsDataTransformer: MetricsDataTransformerFunction = new MetricsDataTransformerFunction(jobConfig, new HttpUtil())

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    BaseMetricsReporter.gaugeMetrics.clear()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "MetricsDataTransformerService" should "execute for valid event" in {
    val inputEvent: Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1),0, 10)
    val map = new ConcurrentHashMap[String, AtomicLong]()
    map.put("success-events-count", new AtomicLong(0))
    val metrics: Metrics = Metrics(map)

    val metricList = Array("me_totalRatingsCount")
    implicit val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(200, """{"id":"api.v3.read.get","ver":"1.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"content":{"parent":"do_123","origin":"do_119831368202241", "originData": {"identifier": "do_2133606182319800321377","repository": "https://dock.preprod.ntp.net.in/api/content/v1/read/do_2133606182319800321377"},"mediaType":"content","name":"APSWREIS-TGT2-Day5","identifier":"do_234","description":"Enter description for Collection","resourceType":"Collection","mimeType":"application/vnd.ekstep.content-collection","contentType":"Collection","language":["English"],"objectType":"Content","status":"Live","idealScreenSize":"normal","contentEncoding":"gzip","osId":"org.ekstep.quiz.app","contentDisposition":"inline","childNodes":["do_345"],"visibility":"Default","pkgVersion":2,"idealScreenDensity":"hdpi"}}}"""))
    when(mockHttpUtil.patch(anyString(), any(), any())).thenReturn(HTTPResponse(200, """{"id":"api.content.hierarchy.get","ver":"3.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"content":{"rootId":"do_123"}}}"""))

    implicit val config = jobConfig
    try {
      metricsDataTransformer.processEvent(inputEvent, metrics, metricList)
    } catch {
      case ex: JobExecutionException =>
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(1)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(0)
      BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(0)
    }
  }

  "MetricsDataTransformerService" should "throw an exception if updating fails" in {
    val inputEvent: Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1),0, 10)
    val map = new ConcurrentHashMap[String, AtomicLong]()
    map.put("failed-events-count", new AtomicLong(0))
    val metrics: Metrics = Metrics(map)

    val metricList = Array("me_totalRatingsCount")
    implicit val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(200, """{"id":"api.v3.read.get","ver":"1.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"content":{"parent":"do_123","origin":"do_119831368202241","originData": {"identifier": "do_2133606182319800321377","repository": "https://dock.preprod.ntp.net.in/api/content/v1/read/do_2133606182319800321377"},"mediaType":"content","name":"APSWREIS-TGT2-Day5","identifier":"do_234","description":"Enter description for Collection","resourceType":"Collection","mimeType":"application/vnd.ekstep.content-collection","contentType":"Collection","language":["English"],"objectType":"Content","status":"Live","idealScreenSize":"normal","contentEncoding":"gzip","osId":"org.ekstep.quiz.app","contentDisposition":"inline","childNodes":["do_345"],"visibility":"Default","pkgVersion":2,"idealScreenDensity":"hdpi"}}}"""))
    implicit val config = jobConfig
    the [Exception] thrownBy {
      metricsDataTransformer.processEvent(inputEvent, metrics, metricList)
    } should have message "Error writing metrics for sourcing id: do_119831368202241"

    when(mockHttpUtil.patch(anyString(), any(), any())).thenReturn(HTTPResponse(404, """{"id":"api.content.hierarchy.get","ver":"3.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"content":{}}}"""))
    the [Exception] thrownBy {
      metricsDataTransformer.processEvent(inputEvent, metrics, metricList)
    } should have message "Error writing metrics for sourcing id: do_119831368202241"

  }

  "MetricsDataTransformerService" should "skip event if response from content read not found" in {
    val inputEvent: Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1),0, 10)
    val map = new ConcurrentHashMap[String, AtomicLong]()
    map.put("skipped-events-count", new AtomicLong(0))
    val metrics: Metrics = Metrics(map)

    val metricList = Array("me_totalRatingsCount")
    implicit val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(404, """{"id":"api.v3.read.get","ver":"1.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"content":{}}}"""))

    implicit val config = jobConfig
    try {
      metricsDataTransformer.processEvent(inputEvent, metrics, metricList)
    } catch {
      case ex: JobExecutionException =>
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(1)
    }
  }

  "MetricsDataTransformerService" should "throw an exception if content read fails" in {
    val inputEvent: Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2),0, 10)
    val map = new ConcurrentHashMap[String, AtomicLong]()
    map.put("failed-events-count", new AtomicLong(0))
    val metrics: Metrics = Metrics(map)

    val metricList = Array("me_totalRatingsCount")
    implicit val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(500, """{"id":"api.v3.read.get","ver":"1.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"ERROR","errmsg":null},"responseCode":"CLIENT_ERROR","result":{"content":{}}}"""))
    implicit val config = jobConfig
    the [Exception] thrownBy {
      metricsDataTransformer.processEvent(inputEvent, metrics, metricList)
    } should have message "Error while getting content data from read API for :: do_113097782275465216111711"
  }

  "MetricsDataTransformerService" should "skip event if sourcing id not present in content read response" in {
    val inputEvent: Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1),0, 10)
    val map = new ConcurrentHashMap[String, AtomicLong]()
    map.put("skipped-events-count", new AtomicLong(0))
    val metrics: Metrics = Metrics(map)

    val metricList = Array("me_totalRatingsCount")
    implicit val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(200, """{"id":"api.v3.read.get","ver":"1.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"content":{"parent":"do_123","mediaType":"content","name":"APSWREIS-TGT2-Day5","identifier":"do_234","description":"Enter description for Collection","resourceType":"Collection","mimeType":"application/vnd.ekstep.content-collection","contentType":"Collection","language":["English"],"objectType":"Content","status":"Live","idealScreenSize":"normal","contentEncoding":"gzip","osId":"org.ekstep.quiz.app","contentDisposition":"inline","childNodes":["do_345"],"visibility":"Default","pkgVersion":2,"idealScreenDensity":"hdpi"}}}"""))

    implicit val config = jobConfig
    try {
      metricsDataTransformer.processEvent(inputEvent, metrics, metricList)
    } catch {
      case ex: JobExecutionException =>
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(1)
    }
  }
}