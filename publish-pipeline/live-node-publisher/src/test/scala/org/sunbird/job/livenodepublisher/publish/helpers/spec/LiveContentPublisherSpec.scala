package org.sunbird.job.livenodepublisher.publish.helpers.spec

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.anyString
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.livenodepublisher.publish.helpers.LiveContentPublisher
import org.sunbird.job.livenodepublisher.task.LiveNodePublisherConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, JanusGraphUtil}
import com.datastax.driver.core.Row

import scala.concurrent.ExecutionContextExecutor

import org.scalatest.Ignore

@Ignore
class LiveContentPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit val cassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: LiveNodePublisherConfig = new LiveNodePublisherConfig(config)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.contentKeyspaceName, jobConfig.contentTableName)
  implicit val cloudStorageUtil: CloudStorageUtil = mock[CloudStorageUtil](Mockito.withSettings().serializable())
  implicit val ec: ExecutionContextExecutor = ExecutionContexts.global
  implicit val defCache: DefinitionCache = new DefinitionCache()
  implicit val defConfig: DefinitionConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)
  implicit val publishConfig: PublishConfig = jobConfig.asInstanceOf[PublishConfig]
  implicit val httpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "enrichObjectMetadata" should "enrich the Content pkgVersion metadata" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/pdf"))
    val result: ObjectData = new TestLiveContentPublisher().enrichObjectMetadata(data).getOrElse(data)
    result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number].intValue() should be(1)
  }

  "enrichObjectMetadata" should "enrich the Content metadata for application/vnd.ekstep.html-archive" in {
    val data = new ObjectData("do_1132167819505500161297", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_1132167819505500161297", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/vnd.ekstep.html-archive", "artifactUrl" -> "artifactUrl.zip"))
    val result: ObjectData = new TestLiveContentPublisher().enrichObjectMetadata(data).getOrElse(data)
    result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number].intValue() should be(1)
  }

  "validateMetadata with invalid external data" should "return exception messages" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]), Some(Map[String, AnyRef]("artifactUrl" -> "artifactUrl")))
    val result: List[String] = new TestLiveContentPublisher().validateMetadata(data, data.identifier, jobConfig)
    result.size should be(1)
  }

  "getExtData " should " get content body for application/vnd.ekstep.ecml-archive mimeType " in {
    val identifier = "do_11321328578759884811663"
    val mockRow = mock[Row]
    when(mockRow.getString("body")).thenReturn("{\"theme\":\"test\"}")
    when(cassandraUtil.findOne(anyString())).thenReturn(mockRow)
    
    val result: Option[ObjectExtData] = new TestLiveContentPublisher().getExtData(identifier, "application/vnd.ekstep.ecml-archive", readerConfig)
    result.getOrElse(new ObjectExtData).data.getOrElse(Map()).contains("body") shouldBe true
  }

  "getExtData " should " not get content body for other than application/pdf mimeType " in {
    val identifier = "do_11321328578759884811663"
    val result: Option[ObjectExtData] = new TestLiveContentPublisher().getExtData(identifier, "application/pdf", readerConfig)
    result.getOrElse(new ObjectExtData).data.getOrElse(Map()).contains("body") shouldBe false
  }

  "getDataForEcar" should "return one element in list" in {
    val data = new ObjectData("do_123", Map("objectType" -> "Content"), Some(Map("responseDeclaration" -> "test")), Some(Map()))
    val result: Option[List[Map[String, AnyRef]]] = new TestLiveContentPublisher().getDataForEcar(data)
    result.get.size should be(1)
  }
}

class TestLiveContentPublisher extends LiveContentPublisher {}
