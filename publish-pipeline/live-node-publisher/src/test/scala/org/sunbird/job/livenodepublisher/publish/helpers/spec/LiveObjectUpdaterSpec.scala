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
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}

import java.util

class LiveObjectUpdaterSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
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

    when(mockNeo4JUtil.executeQuery(anyString())).thenReturn(any());

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val metadata = Map("objectType" -> "QuestionSet", "identifier" -> "do_123","publish_type" -> "Public", "IL_UNIQUE_ID" -> "do_123", "IL_FUNC_OBJECT_TYPE" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))

    val obj = new TestObjectUpdater()
    obj.saveOnSuccess(objData)
  }

  "ObjectUpdater updateProcessingNode" should " update the status for successfully published data " in {

    when(mockNeo4JUtil.executeQuery(anyString())).thenReturn(any());

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val metadata = Map("objectType" -> "QuestionSet", "identifier" -> "do_123","publish_type" -> "Public", "IL_UNIQUE_ID" -> "do_123", "IL_FUNC_OBJECT_TYPE" -> "QuestionSet", "name" -> "Sr.18\\sst\\Grade6%_/@#!-+=()\",?&6.10_ମାଂସପେଶୀ, ରକ୍ତ Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))

    val obj = new TestObjectUpdater()
    obj.updateProcessingNode(objData)
  }

  "ObjectUpdater saveOnFailure" should " update the status for failed published data " in {

    when(mockNeo4JUtil.executeQuery(anyString())).thenReturn(any())

    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val metadata = Map("objectType" -> "QuestionSet","identifier" -> "do_123","publish_type" -> "Public", "IL_UNIQUE_ID" -> "do_123", "IL_FUNC_OBJECT_TYPE" -> "QuestionSet", "name" -> "Test QuestionSet", "status" -> "Live")
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))

    val obj = new TestObjectUpdater()
    obj.saveOnFailure(objData, List("Testing Save on Publish Failure"), 1)
  }

  "ObjectUpdater metaDataQuery" should " give the update query " in {
    val hierarchy = Map("identifier" -> "do_123", "children" -> List(Map("identifier" -> "do_234", "name" -> "Children-1"), Map("identifier" -> "do_345", "name" -> "Children-2")))
    val variants = new util.HashMap[String, String] {{
      put("spine","https://sunbirddev.blob.core.windows.net/sunbird-content-dev/questionset/do_1132380439842324481319/hindi-questionset-17_1615972827743_do_1132380439842324481319_1_SPINE.ecar")
      put("online","https://sunbirddev.blob.core.windows.net/sunbird-content-dev/questionset/do_1132380439842324481319/hindi-questionset-17_1615972829357_do_1132380439842324481319_1_ONLINE.ecar")
    }}
    val outcomeProcessing = Map("name" -> "abc")
    val metadata: Map[String, AnyRef] = Map("description" -> "Hello \"World\"", "keywords" -> List("anusha"),"channel" -> "01309282781705830427","mimeType" -> "application/vnd.sunbird.questionset","showHints" -> "No","objectType" -> "QuestionSet","primaryCategory" -> "Practice Question Set","contentEncoding" -> "gzip","showSolutions" -> "No","identifier" -> "do_1132421849134858241686","visibility" -> "Default","showTimer" -> "Yes","author" -> "anusha","childNodes" -> util.Arrays.asList("do_1132421859856875521687"),"consumerId" -> "273f3b18-5dda-4a27-984a-060c7cd398d3","version" -> 1.asInstanceOf[AnyRef],"prevState" -> "Draft","IL_FUNC_OBJECT_TYPE" -> "QuestionSet","name" -> "Timer","timeLimits" -> "{\"maxTime\":\"3541\",\"warningTime\":\"2401\"}","IL_UNIQUE_ID" -> "do_1132421849134858241686","board" -> "CBSE", "variants" -> variants, "outcomeProcessing" -> outcomeProcessing)
    val objData = new ObjectData("do_123", metadata, None, Some(hierarchy))
    val obj = new TestObjectUpdater()
    println(obj.metaDataQuery(objData)(defCache, definitionConfig))
  }
}

class TestObjectUpdater extends LiveObjectUpdater {
  override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

  override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None
}

