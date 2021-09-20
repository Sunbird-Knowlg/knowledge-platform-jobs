package org.sunbird.job.publish.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.ObjectData
import org.sunbird.job.publish.helpers.{EcarPackageType, ObjectBundle}
import org.sunbird.job.util.HttpUtil

class ObjectBundleSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

	implicit val httpUtil = new HttpUtil

	"validUrl" should "return true for valid url input" in {
		val obj = new TestObjectBundle
		val result = obj.validUrl("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_113105564164997120111/1-vsa-qts-2_1603203738131_do_113105564164997120111_1.0_spine.ecar")
		result should be(true)
	}

	"validUrl" should "return false for invalid url input" in {
		val obj = new TestObjectBundle
		val result = obj.validUrl("http:// abc.com")
		result should be(false)
	}

	"isOnline" should "return true for valid input having contentDisposition online-only" in {
		val obj = new TestObjectBundle
		obj.isOnline("video/mp4", "online-only") should be(true)
	}

	"isOnline" should "return true for valid input having mimeType video/youtube" in {
		val obj = new TestObjectBundle
		obj.isOnline("video/youtube", "inline") should be(true)
	}

	"isOnline" should "return false for invalid input" in {
		val obj = new TestObjectBundle
		obj.isOnline("application/vnd", "inline") should be(false)
	}

	"getBundleFileName" should "return file name with less than 250 characters" in {
		val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
		implicit val publishConfig: PublishConfig = new PublishConfig(config, "")
		val maxAllowedContentName = publishConfig.getInt("max_allowed_content_name", 120)
		val obj = new TestObjectBundle
		val ecarName = obj.getBundleFileName("do_3133687719874150401162",Map("name" -> "6.10_ମାଂସପେଶୀ, ରକ୍ତ ସଂଚାଳନ ଓ ଶ୍ୱସନ ସଂସ୍ଥାନ ଉପରେ ଶାରୀରିକ କାର୍ଯ୍ୟ, ଖେଳ, କ୍ରୀଡା ଓ ଯୋଗର ପ୍ରଭାବ-eng_effects_of_physical_activities_games_sports_and_yoga_on_muscular_circulatory_and_respiratory_systems"), "SPINE")
		(ecarName.length<(maxAllowedContentName+65)) should be(true)
	}

	"getHierarchyFile" should "return valid file" in {
		val obj = new TestObjectBundle
		val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
		val metadata = Map("identifier" -> "do_123", "IL_UNIQUE_ID" -> "do_123", "IL_FUNC_OBJECT_TYPE" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
		val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))
		val file = obj.getHierarchyFile(objData, "/tmp/data").get
		null != file should be(true)
		file.exists() should be(true)
		StringUtils.endsWith(file.getName, "hierarchy.json") should be(true)
	}

	"getManifestFile" should "return valid file" in {
		val obj = new TestObjectBundle
		val objList = List(Map("identifier"->"do_123", "name" -> "Test QuestionSet", "status" -> "Live", "children"->List(Map("identifier" -> "do_234", "name" -> "Children-1"))), Map("identifier"->"do_234", "name" -> "Children-1", "status" -> "Live", "children"->List(Map("identifier" -> "do_345", "name" -> "Children-2"))))
		val file = obj.getManifestFile("do_123", "QuestionSet", "/tmp/data", objList)
		null!=file should be(true)
		file.exists() should be(true)
		StringUtils.endsWith(file.getName, "manifest.json") should be(true)
	}
}

class TestObjectBundle extends ObjectBundle {

}
