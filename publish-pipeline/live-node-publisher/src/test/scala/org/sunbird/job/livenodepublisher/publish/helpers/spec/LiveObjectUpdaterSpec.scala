package org.sunbird.job.livenodepublisher.publish.helpers.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.livenodepublisher.publish.helpers.LiveObjectUpdater
import org.sunbird.job.livenodepublisher.task.LiveNodePublisherConfig
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.util.{CassandraUtil, JanusGraphUtil}

import java.util

class LiveObjectUpdaterSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit val mockCassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  implicit val readerConfig = ExtDataConfig("test", "test")
  implicit lazy val defCache: DefinitionCache = new DefinitionCache()
  implicit val definitionConfig: DefinitionConfig = DefinitionConfig(Map("itemset" -> "2.0"), "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local")
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val jobConfig: LiveNodePublisherConfig = new LiveNodePublisherConfig(config)


  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "ObjectUpdater saveOnSuccess" should " update the status for successfully published data " in {
    Mockito.doNothing().when(mockJanusGraphUtil).updateNode(anyString(), any())

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val metadata = Map("objectType" -> "QuestionSet", "identifier" -> "do_123","publish_type" -> "Public", "IL_UNIQUE_ID" -> "do_123", "IL_FUNC_OBJECT_TYPE" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))

    val obj = new TestObjectUpdater()
    obj.saveOnSuccess(objData)
  }

  "ObjectUpdater saveOnFailure" should " update the status for failed published data " in {
    Mockito.doNothing().when(mockJanusGraphUtil).updateNode(anyString(), any())

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val metadata = Map("objectType" -> "QuestionSet","identifier" -> "do_123","publish_type" -> "Public", "IL_UNIQUE_ID" -> "do_123", "IL_FUNC_OBJECT_TYPE" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))

    val obj = new TestObjectUpdater()
    obj.saveOnFailure(objData, List("Testing Save on Publish Failure"), 1)
  }
}

class TestObjectUpdater extends LiveObjectUpdater {
  override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

  override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None
}

