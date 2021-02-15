package org.sunbird.publish.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.config.PublishConfig
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}
import org.sunbird.publish.helpers.ObjectEnrichment
import org.sunbird.publish.util.CloudStorageUtil

class ObjectEnrichmentSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

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
  implicit val mockCassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  implicit val readerConfig = ExtDataConfig("test", "test")

  "ObjectEnrichment enrichObject" should " enrich the object with Framework data and thumbnail " in {

//    when(mockNeo4JUtil.getNodesName(any[List[String]])).thenReturn(any())
    when(mockNeo4JUtil.getNodesName(List("NCERT"))).thenReturn(Map("NCERT"-> "NCERT"))

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val metadata = Map("identifier" -> "do_123", "targetFWIds" -> List("NCERT"), "boardIds" -> List("NCERT"), "appIcon" -> "https://dev.sunbirded.org/assets/images/sunbird_logo.png", "IL_UNIQUE_ID" -> "do_123", "IL_FUNC_OBJECT_TYPE" -> "QuesstionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))

    val objectEnrichment = new TestObjectEnrichment()
    val result = objectEnrichment.enrichObject(objData)
    val resultMetadata = result.metadata
    resultMetadata.isEmpty should be(false)

    resultMetadata.getOrElse("se_FWIds", List()).asInstanceOf[List[String]].isEmpty should be(false)
    resultMetadata.getOrElse("se_FWIds", List()).asInstanceOf[List[String]].contains("NCERT") should be(true)
    resultMetadata.getOrElse("se_boardIds", List()).asInstanceOf[List[String]].isEmpty should be(false)

    resultMetadata.getOrElse("posterImage", "").asInstanceOf[String].isEmpty should be(false)
    resultMetadata.getOrElse("appIcon", "").asInstanceOf[String].isEmpty should be(false)
  }
}

class TestObjectEnrichment extends ObjectEnrichment {
  override def enrichObjectMetadata(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig): Option[ObjectData] = None
}

