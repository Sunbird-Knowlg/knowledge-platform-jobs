package org.sunbird.job.publish.helpers.spec

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.mockito.Mockito
import org.mockito.Mockito.{doNothing, when, doAnswer}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.knowlg.publish.helpers.QuestionPublisher
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.helpers.EcarPackageType
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, JanusGraphUtil}
import com.datastax.driver.core.Row
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import org.mockito.ArgumentMatchers.{any, anyString}

import java.io.File
import java.util

class QuestionPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit val cassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)
  var definitionCache = new DefinitionCache()
  implicit val definition: ObjectDefinition = definitionCache.getDefinition("Question", jobConfig.schemaSupportVersionMap.getOrElse("question", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.questionKeyspaceName, jobConfig.questionTableName, List("identifier"), definition.getExternalProps)
  implicit val cloudStorageUtil = mock[CloudStorageUtil](Mockito.withSettings().serializable())
  implicit val ec = ExecutionContexts.global
  implicit val defCache = new DefinitionCache()
  implicit val defConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)
  implicit val httpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  implicit val publishConfig: PublishConfig = new PublishConfig(config, "")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val mockRow = mock[Row]
    when(mockRow.getString("body")).thenReturn("body")
    when(mockRow.getString("answer")).thenReturn("answer")
    when(mockRow.getString("editorstate")).thenReturn("editorState")
    when(mockRow.getString("solutions")).thenReturn("solutions")
    when(mockRow.getString("instructions")).thenReturn("instructions")
    when(mockRow.getString("media")).thenReturn("media")
    when(mockRow.getString("hints")).thenReturn("hints")
    
    val rowWithId = mock[Row]
    when(rowWithId.getString("identifier")).thenReturn("do_113188615625731")
    when(rowWithId.getString("body")).thenReturn("body")
    when(rowWithId.getString("answer")).thenReturn("answer")
    when(rowWithId.getString("editorstate")).thenReturn("editorState")
    when(rowWithId.getString("solutions")).thenReturn("solutions")
    when(rowWithId.getString("instructions")).thenReturn("instructions")
    when(rowWithId.getString("media")).thenReturn("media")
    when(rowWithId.getString("hints")).thenReturn("hints")

    val f1 = new File("/tmp/test.pdf")
    if (!f1.exists()) {
      f1.getParentFile.mkdirs()
      f1.createNewFile()
      org.apache.commons.io.FileUtils.writeStringToFile(f1, "test content", "UTF-8")
    }

    when(cassandraUtil.findOne(any())).thenReturn(mockRow)
    when(cassandraUtil.find(any())).thenReturn(java.util.Arrays.asList(rowWithId))
    when(cassandraUtil.upsert(any())).thenReturn(true)
    when(cassandraUtil.executePreparedStatement(any(), any())).thenReturn(java.util.Arrays.asList(mockRow))
    
    when(cloudStorageUtil.uploadFile(any(), any(), any(), any())).thenReturn(Array("test-key", "test-url"))
    when(httpUtil.getSize(any(), any())).thenReturn(100)
    doAnswer(new Answer[File] {
      override def answer(invocation: InvocationOnMock): File = {
        val downloadPath = invocation.getArgument[String](1)
        val file = new File(downloadPath)
        file.getParentFile.mkdirs()
        file.createNewFile()
        file
      }
    }).when(httpUtil).downloadFile(anyString(), anyString())
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "enrichObjectMetadata" should "enrich the Question pkgVersion metadata" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]))
    val result: ObjectData = new TestQuestionPublisher().enrichObjectMetadata(data).getOrElse(data)
    result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number].intValue() should be(1)
  }
  "validateQuestion with invalid external data" should "return exception messages" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]), Some(Map[String, AnyRef]("body" -> "body")))
    val result: List[String] = new TestQuestionPublisher().validateQuestion(data, data.identifier)
    result.size should be(2)
  }

  "saveExternalData " should "save external data to cassandra table" in {
    val data = new ObjectData("do_123", Map[String, AnyRef](), Some(Map[String, AnyRef]("body" -> "body", "answer" -> "answer")))
    new TestQuestionPublisher().saveExternalData(data, readerConfig)
  }

  "getExtData " should "return the external data for the identifier " in {
    val identifier = "do_113188615625731";
    val res: Option[ObjectExtData] = new TestQuestionPublisher().getExtData(identifier, 0.0, "", readerConfig)
    val result: Option[Map[String, AnyRef]] = res.getOrElse(new ObjectExtData).data
    result.getOrElse(Map()).size should be(7)
  }

  "getObjectWithEcar" should "return object with ecar url" in {
    val data = new ObjectData("do_123", Map("objectType" -> "Question", "identifier" -> "do_123", "name" -> "Test Question"), Some(Map("responseDeclaration" -> "test", "media" -> "[{\"id\":\"do_1127129497561497601326\",\"type\":\"image\",\"src\":\"test.pdf\",\"baseUrl\":\"file:///tmp/\"}]")), Some(Map()))
    val result = new TestQuestionPublisher().getObjectWithEcar(data, List(EcarPackageType.FULL.toString, EcarPackageType.ONLINE.toString))(ec, mockJanusGraphUtil, cloudStorageUtil, jobConfig, defCache, defConfig, httpUtil)
    StringUtils.isNotBlank(result.metadata.getOrElse("downloadUrl", "").asInstanceOf[String])
  }
}

class TestQuestionPublisher extends QuestionPublisher {}
