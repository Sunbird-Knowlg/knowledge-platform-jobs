package org.sunbird.job.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.util.{ElasticSearchUtil, HTTPResponse, HttpUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec
import org.sunbird.job.metricstransformer.domain.Event
import org.sunbird.job.metricstransformer.functions.MetricsDataTransformer
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
  lazy val metricsDataTransformer: MetricsDataTransformer = new MetricsDataTransformer(jobConfig, new HttpUtil())

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "MetricsDataTransformerService" should "execute" in {
    val inputEvent: Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1),0, 10)
    val metrics: Metrics = Metrics(new ConcurrentHashMap[String, AtomicLong]())

    val metricList = Array("me_totalRatingsCount")
    implicit val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(200, """{"id":"api.v3.read.get","ver":"1.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"content":{"parent":"do_123","origin":"do_119831368202241","mediaType":"content","name":"APSWREIS-TGT2-Day5","identifier":"do_234","description":"Enter description for Collection","resourceType":"Collection","mimeType":"application/vnd.ekstep.content-collection","contentType":"Collection","language":["English"],"objectType":"Content","status":"Live","idealScreenSize":"normal","contentEncoding":"gzip","osId":"org.ekstep.quiz.app","contentDisposition":"inline","childNodes":["do_345"],"visibility":"Default","pkgVersion":2,"idealScreenDensity":"hdpi"}}}"""))
    when(mockHttpUtil.patch(anyString(), any(), any())).thenReturn(HTTPResponse(200, """{"id":"api.content.hierarchy.get","ver":"3.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"content":{"rootId":"do_123"}}}"""))

    implicit val config = jobConfig
    metricsDataTransformer.processEvent(inputEvent, metrics, metricList)
  }

}