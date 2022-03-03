package org.sunbird.job.publish.helpers.spec

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.questionset.publish.helpers.QuestionPublisher
import org.sunbird.job.questionset.task.QuestionSetPublishConfig
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, Neo4JUtil}

import java.util

class QuestionPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: QuestionSetPublishConfig = new QuestionSetPublishConfig(config)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.questionKeyspaceName, jobConfig.questionTableName)
  implicit val cloudStorageUtil = new CloudStorageUtil(jobConfig)
  implicit val ec = ExecutionContexts.global
  implicit val defCache = new DefinitionCache()
  implicit val defConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)
  implicit val httpUtil = new HttpUtil
  implicit val publishConfig: PublishConfig = new PublishConfig(config, "")

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

  "enrichObjectMetadata" should "enrich the Question pkgVersion metadata" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]))
    val result: ObjectData = new TestQuestionPublisher().enrichObjectMetadata(data).getOrElse(data)
    result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number] should be(1.0.asInstanceOf[Number])
  }
  "validateQuestion with invalid external data" should "return exception messages" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]), Some(Map[String, AnyRef]("body" -> "body")))
    val result: List[String] = new TestQuestionPublisher().validateQuestion(data, data.identifier)
    result.size should be(2)
  }

  "validateQuestion with external data having interaction" should "validate the Question external data" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "interactionTypes" -> new util.ArrayList[String]() {
      add("choice")
    }), Some(Map[String, AnyRef]("body" -> "body", "answer" -> "answer")))
    val result: List[String] = new TestQuestionPublisher().validateQuestion(data, data.identifier)
    result.size should be(3)
  }

  "saveExternalData " should "save external data to cassandra table" in {
    val data = new ObjectData("do_123", Map[String, AnyRef](), Some(Map[String, AnyRef]("body" -> "body", "answer" -> "answer")))
    new TestQuestionPublisher().saveExternalData(data, readerConfig)
  }

  "getExtData " should "return the external data for the identifier " in {
    val identifier = "do_113188615625731";
    val res: Option[ObjectExtData] = new TestQuestionPublisher().getExtData(identifier, 0.0, "", readerConfig)
    val result: Option[Map[String, AnyRef]] = res.getOrElse(new ObjectExtData).data
    result.getOrElse(Map()).size should be(6)
  }

  "getExtData " should "return the external data for the image identifier " in {
    val identifier = "do_113188615625731";
    val res: Option[ObjectExtData] = new TestQuestionPublisher().getExtData(identifier, 1.0, "", readerConfig)
    val result: Option[Map[String, AnyRef]] = res.getOrElse(new ObjectExtData).data
    result.getOrElse(Map()).size should be(7)
  }

  "getHierarchy " should "do nothing " in {
    val identifier = "do_113188615625731";
    new TestQuestionPublisher().getHierarchy(identifier, 1.0, readerConfig)
  }

  "getExtDatas " should "do nothing " in {
    val identifier = "do_113188615625731";
    new TestQuestionPublisher().getExtDatas(List(identifier), readerConfig)
  }

  "getHierarchies " should "do nothing " in {
    val identifier = "do_113188615625731";
    new TestQuestionPublisher().getHierarchies(List(identifier), readerConfig)
  }

  "getDataForEcar" should "return one element in list" in {
    val data = new ObjectData("do_123", Map("objectType" -> "Question"), Some(Map("responseDeclaration" -> "test")), Some(Map()))
    val result: Option[List[Map[String, AnyRef]]] = new TestQuestionPublisher().getDataForEcar(data)
    result.size should be(1)
  }

  "getObjectWithEcar" should "return object with ecar url" in {
    val data = new ObjectData("do_123", Map("objectType" -> "Question", "identifier" -> "do_123", "name" -> "Test Question"), Some(Map("responseDeclaration" -> "test", "media" -> "[{\"id\":\"do_1127129497561497601326\",\"type\":\"image\",\"src\":\"/content/do_1127129497561497601326.img/artifact/sunbird_1551961194254.jpeg\",\"baseUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev\"}]")), Some(Map()))
    val result = new TestQuestionPublisher().getObjectWithEcar(data, List(EcarPackageType.FULL.toString, EcarPackageType.ONLINE.toString))(ec, mockNeo4JUtil, cloudStorageUtil, jobConfig, defCache, defConfig, httpUtil)
    StringUtils.isNotBlank(result.metadata.getOrElse("downloadUrl", "").asInstanceOf[String])

  }

  def delay(time: Long): Unit = {
    try {
      Thread.sleep(time)
    } catch {
      case ex: Exception => print("")
    }
  }
}

class TestQuestionPublisher extends QuestionPublisher {}
