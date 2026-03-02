package org.sunbird.job.publish.util.spec

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.{when, doAnswer}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.knowlg.publish.util.QuestionPublishUtil
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, JanusGraphUtil}
import com.datastax.driver.core.Row
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

import java.io.File
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor

class QuestionPublishUtilSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val ec: ExecutionContextExecutor = ExecutionContexts.global
  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit val cassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)
  var definitionCache = new DefinitionCache()
  implicit val definition: ObjectDefinition = definitionCache.getDefinition("Question", jobConfig.schemaSupportVersionMap.getOrElse("question", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.questionKeyspaceName, jobConfig.questionTableName, List("identifier"), definition.getExternalProps)
  implicit val cloudStorageUtil: CloudStorageUtil = mock[CloudStorageUtil](Mockito.withSettings().serializable())
  implicit val defCache: DefinitionCache = new DefinitionCache()
  implicit val defConfig: DefinitionConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)
  implicit val publishConfig: PublishConfig = new PublishConfig(config, "")
  implicit val httpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val mockRow = mock[Row]
    when(mockRow.getString("body")).thenReturn("body")
    
    val rowWithId = mock[Row]
    when(rowWithId.getString("identifier")).thenReturn("do_113188615625731")
    when(rowWithId.getString("body")).thenReturn("body")

    when(cassandraUtil.findOne(any())).thenAnswer(new Answer[Row] {
      override def answer(invocation: InvocationOnMock): Row = mockRow
    })
    when(cassandraUtil.find(any())).thenAnswer(new Answer[java.util.List[Row]] {
      override def answer(invocation: InvocationOnMock): java.util.List[Row] = java.util.Arrays.asList(rowWithId)
    })
    when(cassandraUtil.upsert(any())).thenReturn(true)
    when(cassandraUtil.executePreparedStatement(any(), any())).thenAnswer(new Answer[java.util.List[Row]] {
      override def answer(invocation: InvocationOnMock): java.util.List[Row] = java.util.Arrays.asList(mockRow)
    })
    
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

  "publishQuestions" should "publish questions in questionset successfully" in {
    when(mockJanusGraphUtil.getNodeProperties("do_113188615625731")).thenReturn(Map[String, AnyRef](
      "identifier" -> "do_113188615625731", 
      "objectType" -> "Question", 
      "IL_FUNC_OBJECT_TYPE" -> "Question", 
      "visibility" -> "Parent", 
      "mimeType" -> "application/vnd.sunbird.question", 
      "primaryCategory" -> "Multiple Choice Question", 
      "name" -> "Test Question", 
      "code" -> "test.question.1",
      "status" -> "Draft"
    ).asJava)

    val identifier = "do_123"
    val obj1: ObjectData = new ObjectData("do_113188615625731", Map[String, AnyRef](
      "identifier" -> "do_113188615625731", 
      "objectType" -> "Question", 
      "visibility" -> "Parent", 
      "mimeType" -> "application/vnd.sunbird.question", 
      "primaryCategory" -> "Multiple Choice Question", 
      "name" -> "Test Question", 
      "code" -> "test.question.1",
      "status" -> "Draft"
    ))
    val objList: List[ObjectData] = List(obj1)
    
    val publishedQuestions = QuestionPublishUtil.publishQuestions(identifier, objList, 1, "test-user")(ec, mockJanusGraphUtil, cassandraUtil, readerConfig, cloudStorageUtil, defCache, defConfig, jobConfig, httpUtil, Map("featureName" -> "test"))
    
    publishedQuestions should not be empty
    publishedQuestions.size should be(1)
    publishedQuestions.head.identifier should be("do_113188615625731")
  }
}
