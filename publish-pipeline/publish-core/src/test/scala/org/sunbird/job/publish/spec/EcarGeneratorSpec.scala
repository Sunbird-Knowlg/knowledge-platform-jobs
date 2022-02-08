package org.sunbird.job.publish.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.util.{CloudStorageUtil, Neo4JUtil, ScalaJsonUtil}
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarGenerator

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
  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap = if (config.hasPath("schema.supportedVersion")) config.getObject("schema.supportedVersion").unwrapped().asScala.toMap else Map[String, AnyRef]()
  implicit val defCache = new DefinitionCache()
  implicit val defConfig = DefinitionConfig(schemaSupportVersionMap, definitionBasePath)

  "Object Ecar Generator generateEcar" should "return a Map containing Packaging Type and its url after uploading it to cloud" in {

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1", "objectType" -> "Question"), Map("identifier" -> "do_345", "name" -> "Children-2", "objectType" -> "Question")))
    val metadata = Map("identifier" -> "do_123", "appIcon" -> "https://dev.sunbirded.org/assets/images/sunbird_logo.png", "identifier" -> "do_123", "objectType" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))
    val obj = new TestEcarGenerator()
    val result = obj.generateEcar(objData,List("SPINE"))
    result.isEmpty should be(false)
  }
}

class TestEcarGenerator extends EcarGenerator {
  val media = Map(
    "id" -> "do_1127129497561497601326",
    "type" -> "image",
    "src" -> "somepath/sunbird_1551961194254.jpeg",
    "baseUrl" -> "some_base_url"
  )
  val testObj = List(Map("children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1", "objectType" -> "Question"), Map("identifier" -> "do_345", "name" -> "Children-2", "objectType" -> "Question")), "name" -> "Test QuestionSet", "appIcon" -> "https://dev.sunbirded.org/assets/images/sunbird_logo.png", "objectType" -> "QuestionSet", "identifier" -> "do_123", "status" -> "Live", "identifier" -> "do_123"), Map("identifier" -> "do_234", "name" -> "Children-1", "objectType" -> "Question", "media" -> ScalaJsonUtil.serialize(List(media))), Map("identifier" -> "do_345", "name" -> "Children-2", "objectType" -> "Question"))
  override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = Some(testObj)
}
