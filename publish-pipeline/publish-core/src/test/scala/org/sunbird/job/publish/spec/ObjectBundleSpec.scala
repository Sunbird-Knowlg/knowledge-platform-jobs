package org.sunbird.job.publish.spec

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.mockito.MockitoSugar.mock
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ObjectData}
import org.sunbird.job.publish.helpers.{EcarPackageType, ObjectBundle}
import org.sunbird.job.util.{HttpUtil, Neo4JUtil}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor

class ObjectBundleSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val httpUtil = new HttpUtil
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val publishConfig: PublishConfig = new PublishConfig(config, "")
  //	implicit val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(publishConfig)
  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit val ec: ExecutionContextExecutor = ExecutionContexts.global
  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap = if (config.hasPath("schema.supportedVersion")) config.getObject("schema.supportedVersion").unwrapped().asScala.toMap else Map[String, AnyRef]()
  implicit val defCache = new DefinitionCache()
  implicit val defConfig = DefinitionConfig(schemaSupportVersionMap, definitionBasePath)

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
    val maxAllowedContentName = publishConfig.getInt("max_allowed_content_name", 120)
    val obj = new TestObjectBundle
    val ecarName = obj.getBundleFileName("do_3133687719874150401162", Map("name" -> "6.10_ମାଂସପେଶୀ, ରକ୍ତ ସଂଚାଳନ ଓ ଶ୍ୱସନ ସଂସ୍ଥାନ ଉପରେ ଶାରୀରିକ କାର୍ଯ୍ୟ, ଖେଳ, କ୍ରୀଡା ଓ ଯୋଗର ପ୍ରଭାବ-eng_effects_of_physical_activities_games_sports_and_yoga_on_muscular_circulatory_and_respiratory_systems"), "SPINE")
    (ecarName.length < (maxAllowedContentName + 65)) should be(true)
  }

  "getBundleFileName" should "return file name" in {
    val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
    implicit val publishConfig: PublishConfig = new PublishConfig(config, "")
    val maxAllowedContentName = publishConfig.getInt("max_allowed_content_name", 120)
    val obj = new TestObjectBundle
    val ecarName = obj.getBundleFileName("do_3133687719874150401162", Map("name" -> "Sr.18\\sst\\Grade6%_/@#!-+=()\",?&6.10_ମାଂସପେଶୀ, ରକ୍ତ ସଂଚାଳନ ଓ ଶ୍ୱସନ ସଂସ୍ଥାନ ଉପରେ ଶାରୀରିକ କାର୍ଯ୍ୟ, ଖେଳ, କ୍ରୀଡା ଓ ଯୋଗର ପ୍ରଭାବ-eng_effects_of_physical_activities_games_sports_and_yoga_on_muscular_circulatory_and_respiratory_systems"), "SPINE")
    (ecarName.length < (maxAllowedContentName + 65)) should be(true)
    (StringUtils.contains(ecarName, "sr.18sstgrade6_-6.10_maanspeshii-rkt-sncaalln-o-shsn-snsthaan-upre-shaariirik-kaaryyy-khell-kriiddaa-o-yogr-prbhaa")) should be(true)
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
    val objList = List(Map("identifier" -> "do_123", "name" -> "Test QuestionSet", "status" -> "Live", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"))), Map("identifier" -> "do_234", "name" -> "Children-1", "status" -> "Live", "children" -> List(Map("identifier" -> "do_345", "name" -> "Children-2"))))
    val file = obj.getManifestFile("do_123", "QuestionSet", "/tmp/data", objList)
    null != file should be(true)
    file.exists() should be(true)
    StringUtils.endsWith(file.getName, "manifest.json") should be(true)
  }

  "getObjectBundle" should " throw exception for invalid artifactUrl " in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]), Some(Map[String, AnyRef]("appIcon" -> "someUrl")))
    val obj = new TestObjectBundle
    val objList = List(Map[String, AnyRef]("publish_type" -> "public", "mediaType" -> "content", "name" -> "PDF Content edited 1", "streamingUrl" -> "http://google.com/fsdfsdfsdf", "identifier" -> "do_11338246158784921611", "primaryCategory" -> "Explanation Content", "framework" -> "NCF", "versionKey" -> "1633945983008", "mimeType" -> "application/pdf", "license" -> "CC BY 4.0", "contentType" -> "ClassroomTeachingVideo", "objectType" -> "Content", "status" -> "Draft", "lastPublishedBy" -> "anil", "contentDisposition" -> "inline", "previewUrl" -> "http://google.com/fsdfsdfsdf", "artifactUrl" -> "http://google.com/fsdfsdfsdf", "visibility" -> "Default"))

    assertThrows[InvalidInputException] {
      obj.getObjectBundle(data, objList, EcarPackageType.FULL)
    }
  }
}

class TestObjectBundle extends ObjectBundle {
}
