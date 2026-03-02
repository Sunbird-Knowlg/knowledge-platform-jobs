package org.sunbird.job.publish.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.ObjectEnrichment
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, JanusGraphUtil}

import org.mockito.ArgumentMatchers.{any, anyBoolean, anyString}
import org.mockito.Mockito.{doNothing, when}
import java.io.File

class ObjectEnrichmentSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    when(cloudStorageUtil.uploadFile(any(), any(), any(), any())).thenReturn(Array("test-key", "test-url"))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val publishConfig: PublishConfig = new PublishConfig(config, "")
  implicit val cloudStorageUtil: CloudStorageUtil = mock[CloudStorageUtil]

  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit val mockCassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  implicit val readerConfig = ExtDataConfig("test", "test")
  implicit lazy val defCache: DefinitionCache = new DefinitionCache()
  implicit lazy val definitionConfig: DefinitionConfig = DefinitionConfig(Map("questionset" -> "1.0"), "https://raw.githubusercontent.com/Sunbird-Knowlg/knowledge-platform/master/schemas/")

  "ObjectEnrichment enrichObject" should " enrich the object with Framework data and thumbnail " in {

//    when(mockJanusGraphUtil.getNodesName(any[List[String]])).thenReturn(any())
    when(mockJanusGraphUtil.getNodesName(List("NCERT"))).thenReturn(Map("NCERT"-> "NCERT"))

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val metadata = Map("identifier" -> "do_123", "targetFWIds" -> List("NCERT"), "boardIds" -> List("NCERT"), "appIcon" -> "https://www.google.com/images/branding/googlelogo/2x/googlelogo_color_272x92dp.png", "IL_UNIQUE_ID" -> "do_123", "IL_FUNC_OBJECT_TYPE" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))

    val objectEnrichment = new TestObjectEnrichment()
    val result = objectEnrichment.enrichObject(objData)
    val resultMetadata = result.metadata
    resultMetadata.isEmpty should be(false)

    resultMetadata.getOrElse("se_FWIds", List()).asInstanceOf[List[String]].isEmpty should be(false)
    resultMetadata.getOrElse("se_FWIds", List()).asInstanceOf[List[String]].contains("NCERT") should be(true)
    resultMetadata.getOrElse("se_boardIds", List()).asInstanceOf[List[String]].isEmpty should be(true)

    resultMetadata.getOrElse("posterImage", "").asInstanceOf[String].isEmpty should be(false)
    resultMetadata.getOrElse("appIcon", "").asInstanceOf[String].isEmpty should be(false)
  }
}

class TestObjectEnrichment extends ObjectEnrichment {
  override def enrichObjectMetadata(obj: ObjectData)(implicit janusGraphUtil: JanusGraphUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig): Option[ObjectData] = None
}

