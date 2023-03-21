package org.sunbird.job.cspmigrator.spec

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.cspmigrator.helpers.URLExtractor

class URLExtractorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {


  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

  }

  "Extractor" should "extract all URLs from the ECML body" in {
    val body: String = "{\"theme\":{\"id\":\"theme\",\"version\":\"1.0\",\"startStage\":\"c3e9064a-9470-40b8-a38a-cd2286a01eeb\",\"stage\":[{\"x\":0,\"y\":0,\"w\":100,\"h\":100,\"id\":\"c3e9064a-9470-40b8-a38a-cd2286a01eeb\",\"rotate\":null,\"config\":{\"__cdata\":\"{\\\"opacity\\\":100,\\\"strokeWidth\\\":1,\\\"stroke\\\":\\\"rgba(255, 255, 255, 0)\\\",\\\"autoplay\\\":false,\\\"visible\\\":true,\\\"color\\\":\\\"#FFFFFF\\\",\\\"instructions\\\":\\\"\\\",\\\"genieControls\\\":false}\"},\"manifest\":{\"media\":[{\"assetId\":\"do_213613202536579072134\"}]},\"image\":[{\"asset\":\"do_213613202536579072134\",\"x\":20,\"y\":20,\"w\":49.72,\"h\":63.31,\"rotate\":0,\"z-index\":0,\"id\":\"ea1dc233-9cbb-4617-b137-be4d8c8c6150\",\"config\":{\"__cdata\":\"{\\\"opacity\\\":100,\\\"strokeWidth\\\":1,\\\"stroke\\\":\\\"rgba(255, 255, 255, 0)\\\",\\\"autoplay\\\":false,\\\"visible\\\":true}\"}}]}],\"manifest\":{\"media\":[{\"id\":\"2288347e-475b-4e93-a72e-e780977e7c13\",\"plugin\":\"org.ekstep.navigation\",\"ver\":\"1.0\",\"src\":\"/content-plugins/org.ekstep.navigation-1.0/renderer/controller/navigation_ctrl.js\",\"type\":\"js\"},{\"id\":\"14164b50-a751-4d44-bae1-8a4d5016266a\",\"plugin\":\"org.ekstep.navigation\",\"ver\":\"1.0\",\"src\":\"/content-plugins/org.ekstep.navigation-1.0/renderer/templates/navigation.html\",\"type\":\"js\"},{\"id\":\"org.ekstep.navigation\",\"plugin\":\"org.ekstep.navigation\",\"ver\":\"1.0\",\"src\":\"/content-plugins/org.ekstep.navigation-1.0/renderer/plugin.js\",\"type\":\"plugin\"},{\"id\":\"org.ekstep.navigation_manifest\",\"plugin\":\"org.ekstep.navigation\",\"ver\":\"1.0\",\"src\":\"/content-plugins/org.ekstep.navigation-1.0/manifest.json\",\"type\":\"json\"},{\"id\":\"do_213613202536579072134\",\"src\":\"https://drive.google.com/uc?export=download&id=1wygNKORDwe6rJ7MTW_ohbUVX4Qw4nWa0\",\"type\":\"image\"}]},\"plugin-manifest\":{\"plugin\":[{\"id\":\"org.ekstep.navigation\",\"ver\":\"1.0\",\"type\":\"plugin\",\"depends\":\"\"}]},\"compatibilityVersion\":2}}"
    val extractor = new TestURLExtractor()
    val extractedURLs: List[String] = extractor.extractUrls(body)
    assert(extractedURLs.nonEmpty)
  }
}


class TestURLExtractor extends URLExtractor {}


