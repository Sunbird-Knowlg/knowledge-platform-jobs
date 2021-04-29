package org.sunbird.job.spec.helper

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.neo4j.driver.v1.StatementResult
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.helpers.ObjectUpdater
import org.sunbird.job.model.{ExtDataConfig, ObjectData}
import org.sunbird.job.task.AutoCreatorV2Config
import org.sunbird.job.util.{CassandraUtil, JSONUtil, Neo4JUtil}

class ObjectUpdaterSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: AutoCreatorV2Config = new AutoCreatorV2Config(config)
  val defCache = new DefinitionCache()
  val qsDefinition: ObjectDefinition = defCache.getDefinition("QuestionSet", jobConfig.schemaSupportVersionMap.getOrElse("questionset", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)
  val qDefinition: ObjectDefinition = defCache.getDefinition("Question", jobConfig.schemaSupportVersionMap.getOrElse("question", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
      delay(10000)
    } catch {
      case ex: Exception => {
      }
    }
  }

  def delay(time: Long): Unit = {
    try {
      Thread.sleep(time)
    } catch {
      case ex: Exception => print("")
    }
  }

  "saveGraphData" should "throw exception" in {
    val data = Map("downloadUrl" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_113252367947718656186/artifact/do_113252367947718656186_1617720706250_gitkraken.png", "artifactUrl" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_113252367947718656186/artifact/do_113252367947718656186_1617720706250_gitkraken.png", "cloudStorageKey" -> "content/do_113252367947718656186/artifact/do_113252367947718656186_1617720706250_gitkraken.png")
    assertThrows[Exception] {
      new TestObjectUpdater().saveGraphData("do_123", data, qsDefinition)(mockNeo4JUtil)
    }
  }

  "saveGraphData" should "return a valid object" in {
    val mockResult = mock[StatementResult](Mockito.withSettings().serializable())
    when(mockNeo4JUtil.executeQuery(anyString())).thenReturn(mockResult)
    val data = Map("downloadUrl" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_113252367947718656186/artifact/do_113252367947718656186_1617720706250_gitkraken.png", "artifactUrl" -> "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_113252367947718656186/artifact/do_113252367947718656186_1617720706250_gitkraken.png", "cloudStorageKey" -> "content/do_113252367947718656186/artifact/do_113252367947718656186_1617720706250_gitkraken.png")
    new TestObjectUpdater().saveGraphData("do_123", data, qsDefinition)(mockNeo4JUtil)
  }

  "saveExternalData" should "throw exception" in {
    val enrObj = new ObjectData("do_123", "Content", Map(), Some(Map("hierarchy" -> getHierarchy())), Some(Map()))
    val extConfig = ExtDataConfig(jobConfig.getString("questionset_keyspace", ""), qDefinition.getExternalTable, qDefinition.getExternalPrimaryKey, qDefinition.getExternalProps)
    assertThrows[Exception] {
      new TestObjectUpdater().saveExternalData("do_123", enrObj.extData.get, extConfig)(cassandraUtil)
    }
  }

  "saveExternalData" should "return a valid object" in {
    val enrObj = new ObjectData("do_123", "Content", Map(), Some(Map("hierarchy" -> getHierarchy())), Some(Map()))
    val extConfig = ExtDataConfig(jobConfig.getString("questionset_keyspace", ""), qsDefinition.getExternalTable, qsDefinition.getExternalPrimaryKey, qsDefinition.getExternalProps)
    new TestObjectUpdater().saveExternalData("do_123", enrObj.extData.get, extConfig)(cassandraUtil)
  }

  "metaDataQuery" should " give the update query " in {
    val variants = new util.HashMap[String, String] {{
      put("spine","https://sunbirddev.blob.core.windows.net/sunbird-content-dev/questionset/do_1132380439842324481319/hindi-questionset-17_1615972827743_do_1132380439842324481319_1_SPINE.ecar")
      put("online","https://sunbirddev.blob.core.windows.net/sunbird-content-dev/questionset/do_1132380439842324481319/hindi-questionset-17_1615972829357_do_1132380439842324481319_1_ONLINE.ecar")
    }}
    val metadata: Map[String, AnyRef] = Map("keywords" -> List("anusha"),"channel" -> "01309282781705830427","mimeType" -> "application/vnd.sunbird.questionset","showHints" -> "No","objectType" -> "QuestionSet","primaryCategory" -> "Practice Question Set","contentEncoding" -> "gzip","showSolutions" -> "No","identifier" -> "do_1132421849134858241686","visibility" -> "Default","showTimer" -> "Yes","author" -> "anusha","childNodes" -> util.Arrays.asList("do_1132421859856875521687"),"consumerId" -> "273f3b18-5dda-4a27-984a-060c7cd398d3","version" -> 1.asInstanceOf[AnyRef],"prevState" -> "Draft","IL_FUNC_OBJECT_TYPE" -> "QuestionSet","name" -> "Timer","timeLimits" -> "{\"maxTime\":\"3541\",\"warningTime\":\"2401\"}","IL_UNIQUE_ID" -> "do_1132421849134858241686","board" -> "CBSE", "variants" -> variants)
    val result = new TestObjectUpdater().metaDataQuery(metadata, qsDefinition)
    result.nonEmpty shouldBe(true)
  }

  def getHierarchy(): Map[String, AnyRef] = {
    JSONUtil.deserialize[Map[String, AnyRef]]("""{"parent":"do_123","children":[{"ownershipType":["createdBy"],"parent":"do_123","questions":[{"identifier":"do_31307327372794265617814","name":"Multilingualism","objectType":"AssessmentItem","relation":"associatedTo","description":null,"index":null,"status":null,"depth":null,"mimeType":null,"visibility":null,"compatibilityLevel":null}],"mimeType":"application/vnd.ekstep.ecml-archive","objectType":"Content","contentType":"Resource","identifier":"do_234","visibility":"Default","mediaType":"content","name":"Test Resource","attributions":["CC BY"],"status":"Live","totalQuestions":1,"resourceType":"Learn"}],"mediaType":"content","name":"Test","identifier":"do_123","resourceType":"Collection","mimeType":"application/vnd.ekstep.content-collection","contentType":"Collection","objectType":"Content","status":"Live","childNodes":[],"visibility":"Default","downloadUrl":"https://dockstorage.blob.core.windows.net/sunbird-content-dock/questionset/do_113244425048121344131/added1_1616751462043_do_113244425048121344131_1_SPINE.ecar"}""")
  }

}

class TestObjectUpdater extends ObjectUpdater {}