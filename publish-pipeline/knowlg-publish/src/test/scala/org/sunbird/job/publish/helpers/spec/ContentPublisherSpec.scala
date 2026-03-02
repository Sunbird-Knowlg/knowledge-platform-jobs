package org.sunbird.job.publish.helpers.spec

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.mockito.Mockito
import org.mockito.Mockito.{doNothing, when, doAnswer}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.knowlg.publish.helpers.ContentPublisher
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.exception.InvalidInputException
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, JanusGraphUtil}
import com.datastax.driver.core.Row
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import org.mockito.ArgumentMatchers.{any, anyString}

import java.io.File
import scala.concurrent.ExecutionContextExecutor

class ContentPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit val cassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)
  var definitionCache = new DefinitionCache()
  implicit val definition: ObjectDefinition = definitionCache.getDefinition("Content", jobConfig.schemaSupportVersionMap.getOrElse("content", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.contentKeyspaceName, jobConfig.contentTableName, List("identifier"), definition.getExternalProps)
  implicit val cloudStorageUtil: CloudStorageUtil = mock[CloudStorageUtil](Mockito.withSettings().serializable())
  implicit val ec: ExecutionContextExecutor = ExecutionContexts.global
  implicit val defCache: DefinitionCache = new DefinitionCache()
  implicit val defConfig: DefinitionConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)
  implicit val publishConfig: PublishConfig = jobConfig.asInstanceOf[PublishConfig]
  implicit val httpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val mockRow = mock[Row]
    when(mockRow.getString("body")).thenReturn("{\"theme\":\"test\"}")
    
    val rowWithId = mock[Row]
    when(rowWithId.getString("identifier")).thenReturn("do_123")
    when(rowWithId.getString("body")).thenReturn("{\"theme\":\"test\"}")

    when(cassandraUtil.findOne(any())).thenReturn(mockRow)
    when(cassandraUtil.find(any())).thenReturn(java.util.Arrays.asList(rowWithId))
    when(cassandraUtil.upsert(any())).thenReturn(true)
    when(cassandraUtil.executePreparedStatement(any(), any())).thenReturn(java.util.Arrays.asList(mockRow))
    
    val f1 = new File("/tmp/test.pdf")
    if (!f1.exists()) { f1.getParentFile.mkdirs(); f1.createNewFile(); org.apache.commons.io.FileUtils.writeStringToFile(f1, "%PDF-1.5 test", "UTF-8") }
    val f2 = new File("/tmp/index.epub")
    if (!f2.exists()) { f2.createNewFile(); org.apache.commons.io.FileUtils.writeStringToFile(f2, "test epub content", "UTF-8") }

    when(cloudStorageUtil.uploadFile(any(), any(), any(), any())).thenReturn(Array("test-key", "test-url"))
    when(httpUtil.getSize(any(), any())).thenReturn(100)
    doAnswer(new Answer[File] {
      override def answer(invocation: InvocationOnMock): File = {
        val downloadPath = invocation.getArgument[String](1)
        val file = new File(downloadPath)
        file.getParentFile.mkdirs()
        if (!file.exists()) file.createNewFile()
        file
      }
    }).when(httpUtil).downloadFile(anyString(), anyString())
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "enrichObjectMetadata" should "enrich the Content pkgVersion metadata" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/pdf"))
    val result: ObjectData = new TestContentPublisher().enrichObjectMetadata(data).getOrElse(data)
    result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number].intValue() should be(1)
  }

  "validateMetadata" should "return errors for invalid data" in {
    val publisher = new TestContentPublisher()
    
    // Each data object has the same ID "do_123" to match mocks, but different metadata
    val data1 = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/pdf", "artifactUrl" -> "file:///tmp/index.epub"), None)
    publisher.validateMetadata(data1, "do_123", jobConfig).nonEmpty should be (true)

    val data2 = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/vnd.ekstep.ecml-archive"), None)
    publisher.validateMetadata(data2, "do_123", jobConfig).nonEmpty should be (true)

    val data3 = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "video/x-youtube", "artifactUrl" -> "https://www.youtube.com/"), None)
    publisher.validateMetadata(data3, "do_123", jobConfig).nonEmpty should be (true)
  }

  "validateMetadata" should "pass for valid data" in {
    val publisher = new TestContentPublisher()
    
    val data1 = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "video/x-youtube", "artifactUrl" -> "https://www.youtube.com/embed/watch?"), None)
    publisher.validateMetadata(data1, "do_123", jobConfig) should be (empty)

    val data2 = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "mimeType" -> "application/pdf", "artifactUrl" -> "file:///tmp/test.pdf"), None)
    publisher.validateMetadata(data2, "do_123", jobConfig) should be (empty)
  }

  "saveExternalData" should "save external data to cassandra table" in {
    val data = new ObjectData("do_123", Map[String, AnyRef](), Some(Map[String, AnyRef]("body" -> "body", "answer" -> "answer")))
    new TestContentPublisher().saveExternalData(data, readerConfig)
  }

  "getExtData" should "get content body for ECML mimeType" in {
    val identifier = "do_123"
    val result: Option[ObjectExtData] = new TestContentPublisher().getExtData(identifier, 0.0, "application/vnd.ekstep.ecml-archive", readerConfig)
    result.getOrElse(new ObjectExtData).data.getOrElse(Map()).contains("body") shouldBe true
  }

  "getObjectWithEcar" should "return object with ecar url" in {
    val data = new ObjectData("do_123", Map("objectType" -> "Content", "identifier" -> "do_123", "name" -> "Test PDF Content"), Some(Map("responseDeclaration" -> "test", "media" -> "[{\"id\":\"do_1127129497561497601326\",\"type\":\"image\",\"src\":\"test.pdf\",\"baseUrl\":\"file:///tmp/\"}]")), Some(Map()))
    val result = new TestContentPublisher().getObjectWithEcar(data, List(EcarPackageType.FULL.toString, EcarPackageType.ONLINE))(ec, mockJanusGraphUtil, cloudStorageUtil, jobConfig, defCache, defConfig, httpUtil)
    StringUtils.isNotBlank(result.metadata.getOrElse("downloadUrl", "").asInstanceOf[String]) should be (true)
  }
}

class TestContentPublisher extends ContentPublisher {}
