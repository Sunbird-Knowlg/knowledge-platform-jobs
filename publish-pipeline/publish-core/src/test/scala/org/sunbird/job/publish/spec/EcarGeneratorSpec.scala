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
import org.sunbird.job.publish.helpers.EcarGenerator

import java.io.File
import java.nio.file.Files
import java.security.MessageDigest
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

  "computeArtifactHash" should "return artifactHash with no prevArtifactHash on first publish" in {
    val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
    Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(null)
    val obj = new TestEcarGenerator()
    val metadata = Map("identifier" -> "do_123")
    val objData = new ObjectData("do_123", metadata, None, None)
    val artifactFile = createTempFile("first-publish-content")

    val result = obj.computeArtifactHash(objData, artifactFile)(mockJanusGraphUtil)

    result should be(Some((sha256Hex("first-publish-content"), None)))
  }

  it should "produce the same hash for identical bytes" in {
    val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
    Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(null)
    val obj = new TestEcarGenerator()
    val objData = new ObjectData("do_123", Map("identifier" -> "do_123"), None, None)
    val fileA = createTempFile("identical-content")
    val fileB = createTempFile("identical-content")

    val resultA = obj.computeArtifactHash(objData, fileA)(mockJanusGraphUtil)
    val resultB = obj.computeArtifactHash(objData, fileB)(mockJanusGraphUtil)

    resultA.map(_._1) should be(resultB.map(_._1))
    resultA.map(_._1) should be(Some(sha256Hex("identical-content")))
  }

  it should "carry forward the previous hash as prevArtifactHash on republish" in {
    val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
    val existingProps: java.util.Map[String, AnyRef] = new java.util.HashMap[String, AnyRef]()
    existingProps.put("artifactHash", "oldhash123")
    Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(existingProps)
    val obj = new TestEcarGenerator()
    val metadata = Map("identifier" -> "do_123")
    val objData = new ObjectData("do_123", metadata, None, None)
    val artifactFile = createTempFile("republish-content")

    val result = obj.computeArtifactHash(objData, artifactFile)(mockJanusGraphUtil)

    result.flatMap(_._2) should be(Some("oldhash123"))
  }

  it should "produce different hashes for two same-size files with different content" in {
    val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
    Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(null)
    val obj = new TestEcarGenerator()
    val objData = new ObjectData("do_123", Map("identifier" -> "do_123"), None, None)
    val fileA = createTempFile("aaaaaaaaaa")
    val fileB = createTempFile("bbbbbbbbbb")

    val resultA = obj.computeArtifactHash(objData, fileA)(mockJanusGraphUtil)
    val resultB = obj.computeArtifactHash(objData, fileB)(mockJanusGraphUtil)

    resultA.map(_._1) should not be resultB.map(_._1)
    resultA.map(_._1) should be(Some(sha256Hex("aaaaaaaaaa")))
    resultB.map(_._1) should be(Some(sha256Hex("bbbbbbbbbb")))
  }

  it should "log and swallow the error instead of throwing when reading current node properties fails" in {
    val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
    Mockito.when(mockJanusGraphUtil.getNodeProperties(anyString())).thenThrow(new RuntimeException("read failed"))
    val obj = new TestEcarGenerator()
    val objData = new ObjectData("do_123", Map("identifier" -> "do_123"), None, None)
    val artifactFile = createTempFile("content")

    noException should be thrownBy obj.computeArtifactHash(objData, artifactFile)(mockJanusGraphUtil)
  }

  // Computed independently of EcarGenerator's own digest logic, so the assertion
  // actually pins the expected SHA-256 value rather than re-deriving it the same way.
  private def sha256Hex(content: String): String =
    MessageDigest.getInstance("SHA-256").digest(content.getBytes).map("%02x".format(_)).mkString

  private def createTempFile(content: String): File = {
    val file = File.createTempFile("artifact-hash-spec", ".tmp")
    file.deleteOnExit()
    Files.write(file.toPath, content.getBytes)
    file
  }
}

class TestEcarGenerator extends EcarGenerator {
  override def computeArtifactHash(obj: ObjectData, artifactFile: File)(implicit janusGraphUtil: JanusGraphUtil): Option[(String, Option[String])] =
    super.computeArtifactHash(obj, artifactFile)
  val media = Map(
    "id" -> "do_1127129497561497601326",
    "type" -> "image",
    "src" -> "somepath/sunbird_1551961194254.jpeg",
    "baseUrl" -> "some_base_url"
  )
  val testObj = List(Map("children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1", "objectType" -> "Question"), Map("identifier" -> "do_345", "name" -> "Children-2", "objectType" -> "Question")), "name" -> "Test QuestionSet", "appIcon" -> "https://dev.sunbirded.org/content/preview/assets/icons/avatar_anonymous.png", "objectType" -> "QuestionSet", "identifier" -> "do_123", "status" -> "Live", "identifier" -> "do_123"), Map("identifier" -> "do_234", "name" -> "Children-1", "objectType" -> "Question", "media" -> ScalaJsonUtil.serialize(List(media))), Map("identifier" -> "do_345", "name" -> "Children-2", "objectType" -> "Question"))
  override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = Some(testObj)
}
