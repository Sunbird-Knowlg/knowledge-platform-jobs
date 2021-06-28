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
import org.sunbird.job.content.publish.helpers.ContentPublisher
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.util.CloudStorageUtil
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}

import java.util

class ContentPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: ContentPublishConfig = new ContentPublishConfig(config)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.contentKeyspaceName, jobConfig.contentTableName)
  implicit val cloudStorageUtil = new CloudStorageUtil(jobConfig)
  implicit val ec = ExecutionContexts.global
  implicit val defCache = new DefinitionCache()
  implicit val defConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)

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

  "enrichObjectMetadata" should "enrich the Content pkgVersion metadata" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]))
    val result: ObjectData = new TestContentPublisher().enrichObjectMetadata(data).getOrElse(data)
    result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number] should be(1.0.asInstanceOf[Number])
  }
  "validateMetadata with invalid external data" should "return exception messages" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]), Some(Map[String, AnyRef]("body" -> "body")))
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier)
    result.size should be(2)
  }

  "validateMetadata with external data having interaction" should "validate the Content external data" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "interactionTypes" -> new util.ArrayList[String]() {add("choice")}), Some(Map[String, AnyRef]("body" -> "body", "answer" -> "answer")))
    val result: List[String] = new TestContentPublisher().validateMetadata(data, data.identifier)
    result.size should be(3)
  }

  "saveExternalData " should "save external data to cassandra table" in {
    val data = new ObjectData("do_123", Map[String, AnyRef](), Some(Map[String, AnyRef]("body" -> "body", "answer" -> "answer")))
    new TestContentPublisher().saveExternalData(data, readerConfig)
  }

  "getExtData " should "return the external data for the identifier " in {
    val identifier = "do_113188615625731";
    val result: Option[Map[String, AnyRef]] = new TestContentPublisher().getExtData(identifier, 0.0, readerConfig)
    result.getOrElse(Map()).size should be(6)
  }

  "getExtData " should "return the external data for the image identifier " in {
    val identifier = "do_113188615625731";
    val result: Option[Map[String, AnyRef]] = new TestContentPublisher().getExtData(identifier, 1.0, readerConfig)
    result.getOrElse(Map()).size should be(7)
  }

  "getHierarchy " should "do nothing " in {
    val identifier = "do_113188615625731";
    new TestContentPublisher().getExtData(identifier, 1.0, readerConfig)
  }

  "getExtDatas " should "do nothing " in {
    val identifier = "do_113188615625731";
    new TestContentPublisher().getExtDatas(List(identifier), readerConfig)
  }

  "getHierarchies " should "do nothing " in {
    val identifier = "do_113188615625731";
    new TestContentPublisher().getHierarchies(List(identifier), readerConfig)
  }

  "getDataForEcar" should "return one element in list" in {
    val data = new ObjectData("do_123", Map("objectType"->"Content"), Some(Map("responseDeclaration"->"test")), Some(Map()))
    val result: Option[List[Map[String, AnyRef]]] = new TestContentPublisher().getDataForEcar(data)
    result.size should be (1)
  }

  "getObjectWithEcar" should "return object with ecar url" in {
    //val media = List(Map("id"->"do_1127129497561497601326", "type"->"image","src"->"/content/do_1127129497561497601326.img/artifact/sunbird_1551961194254.jpeg","baseUrl"->"https://sunbirddev.blob.core.windows.net/sunbird-content-dev"))
    val data = new ObjectData("do_123", Map("objectType" -> "Content", "identifier"->"do_123", "name"->"Test PDF Content"), Some(Map("responseDeclaration" -> "test", "media"->"[{\"id\":\"do_1127129497561497601326\",\"type\":\"image\",\"src\":\"/content/do_1127129497561497601326.img/artifact/sunbird_1551961194254.jpeg\",\"baseUrl\":\"https://sunbirddev.blob.core.windows.net/sunbird-content-dev\"}]")), Some(Map()))
    val result = new TestContentPublisher().getObjectWithEcar(data, List("FULL", "ONLINE"))(ec, cloudStorageUtil, defCache, defConfig)
    StringUtils.isNotBlank(result.metadata.getOrElse("downloadUrl", "").asInstanceOf[String])
  }
}

class TestContentPublisher extends ContentPublisher {}
