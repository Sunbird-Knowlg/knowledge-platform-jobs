package org.sunbird.job.publish.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito
import org.mockito.ArgumentMatchers.anyString
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.util.{CloudStorageUtil, JanusGraphUtil, ScalaJsonUtil}
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ObjectData}
import org.sunbird.job.publish.helpers.{EcarGenerator, EcarResult}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

class EcarGeneratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val publishConfig: PublishConfig = new PublishConfig(config, "")
  implicit val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(publishConfig)
  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap = if (config.hasPath("schema.supportedVersion")) config.getObject("schema.supportedVersion").unwrapped().asScala.toMap else Map[String, AnyRef]()
  implicit val defCache = new DefinitionCache()
  implicit val defConfig = DefinitionConfig(schemaSupportVersionMap, definitionBasePath)

  "Object Ecar Generator generateEcar" should "return a Map containing Packaging Type and its url after uploading it to cloud" in {

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1", "objectType" -> "Question"), Map("identifier" -> "do_345", "name" -> "Children-2", "objectType" -> "Question")))
    val metadata = Map("identifier" -> "do_123", "appIcon" -> "https://dev.sunbirded.org/content/preview/assets/icons/avatar_anonymous.png", "identifier" -> "do_123", "objectType" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))
    val obj = new TestEcarGenerator()
    val result = obj.generateEcar(objData,List("SPINE"))
    result.urls.isEmpty should be(false)
  }

  "readPrevArtifactHash" should "return None when the node has no prior artifactHash" in {
    val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
    Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(null)
    val obj = new TestEcarGenerator()
    val objData = new ObjectData("do_123", Map("identifier" -> "do_123"), None, None)

    obj.readPrevArtifactHash(objData)(mockJanusGraphUtil) should be(None)
  }

  it should "return the existing artifactHash as prevArtifactHash on republish" in {
    val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
    val existingProps: java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]()
    existingProps.put("artifactHash", "oldhash123")
    Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(existingProps)
    val obj = new TestEcarGenerator()
    val objData = new ObjectData("do_123", Map("identifier" -> "do_123"), None, None)

    obj.readPrevArtifactHash(objData)(mockJanusGraphUtil) should be(Some("oldhash123"))
  }

  it should "log and swallow the error instead of throwing when reading current node properties fails" in {
    val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
    Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenThrow(new RuntimeException("read failed"))
    val obj = new TestEcarGenerator()
    val objData = new ObjectData("do_123", Map("identifier" -> "do_123"), None, None)

    noException should be thrownBy obj.readPrevArtifactHash(objData)(mockJanusGraphUtil)
  }

  "EcarResult.hashMeta" should "include both artifactHash and prevArtifactHash when both are present" in {
    EcarResult(Map("FULL" -> "url"), Some("newhash"), Some("oldhash")).hashMeta should be(Map("artifactHash" -> "newhash", "prevArtifactHash" -> "oldhash"))
  }

  it should "omit prevArtifactHash entirely rather than writing it as null on first publish" in {
    EcarResult(Map("FULL" -> "url"), Some("newhash"), None).hashMeta should be(Map("artifactHash" -> "newhash"))
  }

  it should "be empty when no artifact was hashed this round" in {
    EcarResult(Map("FULL" -> "url"), None, None).hashMeta should be(Map.empty)
  }
}

class TestEcarGenerator extends EcarGenerator {
  override def readPrevArtifactHash(obj: ObjectData)(implicit janusGraphUtil: JanusGraphUtil): Option[String] =
    super.readPrevArtifactHash(obj)
  val media = Map(
    "id" -> "do_1127129497561497601326",
    "type" -> "image",
    "src" -> "somepath/sunbird_1551961194254.jpeg",
    "baseUrl" -> "some_base_url"
  )
  val testObj = List(Map("children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1", "objectType" -> "Question"), Map("identifier" -> "do_345", "name" -> "Children-2", "objectType" -> "Question")), "name" -> "Test QuestionSet", "appIcon" -> "https://dev.sunbirded.org/content/preview/assets/icons/avatar_anonymous.png", "objectType" -> "QuestionSet", "identifier" -> "do_123", "status" -> "Live", "identifier" -> "do_123"), Map("identifier" -> "do_234", "name" -> "Children-1", "objectType" -> "Question", "media" -> ScalaJsonUtil.serialize(List(media))), Map("identifier" -> "do_345", "name" -> "Children-2", "objectType" -> "Question"))
  override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = Some(testObj)
}
