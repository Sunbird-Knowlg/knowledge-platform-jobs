package org.sunbird.job.contentautocreator.spec.helper

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.contentautocreator.helpers.ContentCollectionUpdater
import org.sunbird.job.contentautocreator.task.ContentAutoCreatorConfig
import org.sunbird.job.util.{HTTPResponse, HttpUtil}

class CollectionUpdaterSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: ContentAutoCreatorConfig = new ContentAutoCreatorConfig(config)
  val mockHttpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())

  "linkCollection" should "validate and update the collection hierarchy" in {
    val collection: List[Map[String, AnyRef]] = List(Map("identifier" -> "do_234", "unitId" -> "do_345"))
    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(200, """{"id":"api.content.hierarchy.get","ver":"3.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"content":{"parent":"do_123","mediaType":"content","name":"APSWREIS-TGT2-Day5","identifier":"do_234","description":"Enter description for Collection","resourceType":"Collection","mimeType":"application/vnd.ekstep.content-collection","contentType":"Collection","language":["English"],"objectType":"Content","status":"Live","idealScreenSize":"normal","contentEncoding":"gzip","osId":"org.ekstep.quiz.app","contentDisposition":"inline","childNodes":["do_345"],"visibility":"Default","pkgVersion":2,"idealScreenDensity":"hdpi"}}}"""))
    when(mockHttpUtil.patch(anyString(), any(), any())).thenReturn(HTTPResponse(200, """{"id":"api.content.hierarchy.get","ver":"3.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"content":{"rootId":"do_123"}}}"""))
    new TestContentCollectionUpdater().linkCollection("do_123", collection)(jobConfig, mockHttpUtil)
  }

  "getHierarchy with invalid identifier" should "throw Exception" in {
    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(404, """{"id":"api.content.hierarchy.get","ver":"3.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"failed","errmsg":null},"responseCode":"RESOURCE_NOT_FOUND","result":{}}"""))
    intercept[Exception] {
      new TestContentCollectionUpdater().getHierarchy("do_123")(jobConfig, mockHttpUtil)
    }
  }

  "addToHierarchy with invalid identifier" should "throw Exception" in {
    when(mockHttpUtil.patch(anyString(), any(), any())).thenReturn(HTTPResponse(400, """{"id":"api.content.hierarchy.get","ver":"3.0","ts":"2020-08-07T15:56:47ZZ","params":{"resmsgid":"bbd98348-c70b-47bc-b9f1-396c5e803436","msgid":null,"err":null,"status":"failed","errmsg":null},"responseCode":"RESOURCE_NOT_FOUND","result":{}}"""))
    intercept[Exception] {
      new TestContentCollectionUpdater().addToHierarchy("do_123", "do_234", "do_345")(jobConfig, mockHttpUtil)
    }
  }
}

class TestContentCollectionUpdater extends ContentCollectionUpdater {}

