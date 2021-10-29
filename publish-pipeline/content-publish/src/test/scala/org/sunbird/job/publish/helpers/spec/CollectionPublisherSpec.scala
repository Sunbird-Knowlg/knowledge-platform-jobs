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
import org.sunbird.job.content.publish.helpers.CollectionPublisher
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, Neo4JUtil}

import scala.concurrent.ExecutionContextExecutor

class CollectionPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: ContentPublishConfig = new ContentPublishConfig(config)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.hierarchyKeyspaceName, jobConfig.hierarchyTableName)
  implicit val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(jobConfig)
  implicit val ec: ExecutionContextExecutor = ExecutionContexts.global
  implicit val defCache: DefinitionCache = new DefinitionCache()
  implicit val defConfig: DefinitionConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)
  implicit val publishConfig: PublishConfig = jobConfig.asInstanceOf[PublishConfig]
  implicit val httpUtil: HttpUtil = new HttpUtil

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
      case ex: Exception =>
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
    val data = new ObjectData("do_2133950809948078081503", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_2133950809948078081503", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/vnd.ekstep.content-collection"))
    val result: ObjectData = new TestCollectionPublisher().enrichObjectMetadata(data).getOrElse(data)
    result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number] should be(1.0.asInstanceOf[Number])
  }

  //  "validateMetadata with invalid external data" should "return exception messages" in {
  //    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]), Some(Map[String, AnyRef]("artifactUrl" -> "artifactUrl")))
  //    val result: List[String] = new TestCollectionPublisher().validateMetadata(data, data.identifier)
  //    result.size should be(1)
  //  }

  "saveExternalData " should "save external data to cassandra table" in {
    val data = new ObjectData("do_123", Map[String, AnyRef](), Some(Map[String, AnyRef]("body" -> "body", "answer" -> "answer")))
    new TestCollectionPublisher().saveExternalData(data, readerConfig)
  }

  "getHierarchy " should "do nothing " in {
    val identifier = "do_11329603741667328018"
    new TestCollectionPublisher().getHierarchy(identifier, 1.0, readerConfig)
  }

  "getExtDatas " should "do nothing " in {
    val identifier = "do_11329603741667328018"
    new TestCollectionPublisher().getExtDatas(List(identifier), readerConfig)
  }

  "getHierarchies " should "do nothing " in {
    val identifier = "do_11329603741667328018"
    new TestCollectionPublisher().getHierarchies(List(identifier), readerConfig)
  }

  "getDataForEcar" should "return one element in list" in {
    val data = new ObjectData("do_123", Map("objectType" -> "Content"), Some(Map("responseDeclaration" -> "test")), Some(Map()))
    val result: Option[List[Map[String, AnyRef]]] = new TestCollectionPublisher().getDataForEcar(data)
    result.size should be(1)
  }

  "getObjectWithEcar" should "return object with ecar url" in {
    val data = new ObjectData("do_123", Map("objectType" -> "Collection", "identifier" -> "do_123", "name" -> "Test Collection"), Some(Map()), Some(Map()))
    val result = new TestCollectionPublisher().getObjectWithEcar(data, List(EcarPackageType.SPINE.toString, EcarPackageType.ONLINE.toString))(ec, mockNeo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, jobConfig, defCache, defConfig, httpUtil)
    StringUtils.isNotBlank(result.metadata.getOrElse("downloadUrl", "").asInstanceOf[String])
  }

}

class TestCollectionPublisher extends CollectionPublisher {}
