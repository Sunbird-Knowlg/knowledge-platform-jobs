package org.sunbird.job.publish.util.spec

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.knowlg.publish.util.QuestionPublishUtil
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, JanusGraphUtil}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor

class QuestionPublishUtilSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val ec: ExecutionContextExecutor = ExecutionContexts.global
  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.questionKeyspaceName, jobConfig.questionTableName)
  implicit val cloudStorageUtil: CloudStorageUtil = new CloudStorageUtil(jobConfig)
  implicit val defCache: DefinitionCache = new DefinitionCache()
  implicit val defConfig: DefinitionConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)
  implicit val publishConfig: PublishConfig = new PublishConfig(config, "")
  implicit val httpUtil: HttpUtil = new HttpUtil

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort, jobConfig)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      if (cassandraUtil != null) {
        cassandraUtil.close()
      }
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

  "publishQuestions" should "handle questions with lastPublishedBy" in {
    when(mockJanusGraphUtil.getNodeProperties("do_113188615625732")).thenReturn(Map[String, AnyRef](
      "identifier" -> "do_113188615625732", 
      "objectType" -> "Question", 
      "IL_FUNC_OBJECT_TYPE" -> "Question", 
      "visibility" -> "Parent", 
      "mimeType" -> "application/vnd.sunbird.question", 
      "primaryCategory" -> "Multiple Choice Question", 
      "name" -> "Test Question 2", 
      "code" -> "test.question.2",
      "status" -> "Draft"
    ).asJava)

    val identifier = "do_124"
    val obj1: ObjectData = new ObjectData("do_113188615625732", Map[String, AnyRef](
      "identifier" -> "do_113188615625732", 
      "objectType" -> "Question", 
      "visibility" -> "Parent", 
      "mimeType" -> "application/vnd.sunbird.question", 
      "primaryCategory" -> "Multiple Choice Question", 
      "name" -> "Test Question 2", 
      "code" -> "test.question.2",
      "status" -> "Draft"
    ))
    val objList: List[ObjectData] = List(obj1)
    
    val publishedQuestions = QuestionPublishUtil.publishQuestions(identifier, objList, 1, "publisher-user")(ec, mockJanusGraphUtil, cassandraUtil, readerConfig, cloudStorageUtil, defCache, defConfig, jobConfig, httpUtil, Map("featureName" -> "test"))
    
    publishedQuestions should not be empty
    publishedQuestions.size should be(1)
    publishedQuestions.head.metadata should contain key "lastPublishedBy"
    publishedQuestions.head.metadata.getOrElse("lastPublishedBy", "") should be("publisher-user")
  }

  "publishQuestions" should "handle empty question list" in {
    val identifier = "do_125"
    val objList: List[ObjectData] = List()
    
    val publishedQuestions = QuestionPublishUtil.publishQuestions(identifier, objList, 1, "test-user")(ec, mockJanusGraphUtil, cassandraUtil, readerConfig, cloudStorageUtil, defCache, defConfig, jobConfig, httpUtil, Map("featureName" -> "test"))
    
    publishedQuestions should be(empty)
  }

  "publishQuestions" should "handle questions without artifactUrl" in {
    when(mockJanusGraphUtil.getNodeProperties("do_113188615625733")).thenReturn(Map[String, AnyRef](
      "identifier" -> "do_113188615625733", 
      "objectType" -> "Question", 
      "IL_FUNC_OBJECT_TYPE" -> "Question", 
      "visibility" -> "Parent", 
      "mimeType" -> "application/vnd.sunbird.question", 
      "primaryCategory" -> "Multiple Choice Question", 
      "name" -> "Test Question 3", 
      "code" -> "test.question.3",
      "status" -> "Draft",
      "artifactUrl" -> ""
    ).asJava)

    val identifier = "do_126"
    val obj1: ObjectData = new ObjectData("do_113188615625733", Map[String, AnyRef](
      "identifier" -> "do_113188615625733", 
      "objectType" -> "Question", 
      "visibility" -> "Parent", 
      "mimeType" -> "application/vnd.sunbird.question", 
      "primaryCategory" -> "Multiple Choice Question", 
      "name" -> "Test Question 3", 
      "code" -> "test.question.3",
      "status" -> "Draft",
      "artifactUrl" -> ""
    ))
    val objList: List[ObjectData] = List(obj1)
    
    val publishedQuestions = QuestionPublishUtil.publishQuestions(identifier, objList, 1, "test-user")(ec, mockJanusGraphUtil, cassandraUtil, readerConfig, cloudStorageUtil, defCache, defConfig, jobConfig, httpUtil, Map("featureName" -> "test"))
    
    publishedQuestions should not be empty
    publishedQuestions.size should be(1)
    // The artifact URL should be generated during publishing
    publishedQuestions.head.metadata should contain key "artifactUrl"
  }
}