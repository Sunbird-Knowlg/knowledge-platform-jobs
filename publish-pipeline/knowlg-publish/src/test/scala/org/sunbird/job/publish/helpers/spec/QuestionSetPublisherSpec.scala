package org.sunbird.job.publish.helpers.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito
import org.mockito.Mockito.{when, doAnswer}
import org.mockito.ArgumentMatchers.{any, anyString, contains}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.knowlg.publish.helpers.QuestionSetPublisher
import org.sunbird.job.knowlg.task.KnowlgPublishConfig
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, JanusGraphUtil}
import com.datastax.driver.core.Row
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

import java.util
import java.io.File

class QuestionSetPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit val cassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)
  implicit val cloudStorageUtil: CloudStorageUtil = mock[CloudStorageUtil]
  implicit val httpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  var definitionCache = new DefinitionCache()
  
  implicit val definition: ObjectDefinition = definitionCache.getDefinition("QuestionSet", jobConfig.schemaSupportVersionMap.getOrElse("questionset", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)
  val qDefinition: ObjectDefinition = definitionCache.getDefinition("Question", jobConfig.schemaSupportVersionMap.getOrElse("question", "1.0").asInstanceOf[String], jobConfig.definitionBasePath)
  
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.questionSetKeyspaceName, jobConfig.questionSetTableName, List("identifier"), definition.getExternalProps)
  val questionReaderConfig: ExtDataConfig = ExtDataConfig(jobConfig.questionKeyspaceName, jobConfig.questionTableName, List("identifier"), qDefinition.getExternalProps)
  
  implicit val defCache = new DefinitionCache()
  implicit val defConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val mockRow = mock[Row]
    when(mockRow.getString("hierarchy")).thenReturn("""{"identifier":"do_123","children":[{"identifier":"do_113188615625731"},{"identifier":"do_2"}],"status":"Live"}""")
    when(mockRow.getString("body")).thenReturn("{}")
    
    val rowWithId = mock[Row]
    when(rowWithId.getString("identifier")).thenReturn("do_113188615625731")
    when(rowWithId.getString("body")).thenReturn("{}")
    when(rowWithId.getString("hierarchy")).thenReturn("""{"identifier":"do_113188615625731","status":"Live"}""")

    val row1Child = mock[Row]
    when(row1Child.getString("hierarchy")).thenReturn("""{"identifier":"do_321","children":[{"identifier":"do_3"}],"status":"Live"}""")

    when(cassandraUtil.findOne(any())).thenAnswer(new Answer[Row] {
      override def answer(invocation: InvocationOnMock): Row = {
        val query = invocation.getArgument[Any](0).toString
        if (query.contains("do_321")) row1Child
        else if (query.contains("do_113188615625731")) rowWithId
        else mockRow
      }
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

  "validateQuestionSet with no hierarchy, mimeType and visibility" should "return 3 error message" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]))
    val result: List[String] = new TestQuestionSetPublisher().validateQuestionSet(data, data.identifier)
    result.size should be(3)
  }

  "saveExternalData" should "save external data like hierarchy" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]))
    new TestQuestionSetPublisher().saveExternalData(data, readerConfig)
  }

  "populateChildrenMapRecursively with two children" should "return a map with one data" in {
    val result: Map[String, AnyRef] = new TestQuestionSetPublisher().populateChildrenMapRecursively(List(Map[String, AnyRef] ("identifier" -> "do_123", "objectType" -> "Question"), Map[String, AnyRef] ("identifier" -> "do_124", "objectType" -> "QuestionSet")), Map())
    result.size should be (1)
  }

  "getDataForEcar with empty children" should "return one element in list" in {
    val data = new ObjectData("do_123", Map(), Some(Map()), Some(Map[String, AnyRef]("identifier" -> "do_123", "children" -> List(Map()))))
    val result: Option[List[Map[String, AnyRef]]] = new TestQuestionSetPublisher().getDataForEcar(data)
    result.size should be (1)
  }

  "enrichObjectMetadata with valid children" should "return updated pkgVersion" in {
    val data = new ObjectData("do_123", Map[String, AnyRef] ("name" -> "QS1", "objectType" -> "QuestionSet", "status" -> "Draft"), Some(Map()), Some(Map[String, AnyRef]("identifier" -> "do_123", "children" -> List(Map[String, AnyRef]("identifier" -> "do_124", "objectType"->"QuestionSet", "visibility"-> "Parent"), Map[String, AnyRef]("identifier" -> "do_113188615625731", "objectType"->"Question", "visibility"-> "Default")))))
    val result: Option[ObjectData] = new TestQuestionSetPublisher().enrichObjectMetadata(data)
    result.getOrElse(new ObjectData("do_123", Map())).pkgVersion should be (1.asInstanceOf[Number].doubleValue())
    result.size should be (1)
  }

  "getHierarchy " should " return the hierarchy of the Question Set for the image id " in {
    val identifier = "do_123"
    val result =  new TestQuestionSetPublisher().getHierarchy(identifier, 1.0, readerConfig)
    result.getOrElse(Map()).size should be(3)
    result.getOrElse(Map()).getOrElse("children", List(Map())).asInstanceOf[List[Map[String, AnyRef]]].size should be (2)
  }

  "getHierarchy " should " return the hierarchy of the Question Set " in {
    val identifier = "do_321"
    val result =  new TestQuestionSetPublisher().getHierarchy(identifier, 1.0, readerConfig)
    result.getOrElse(Map()).size should be(3)
    result.getOrElse(Map()).getOrElse("children", List(Map())).asInstanceOf[List[Map[String, AnyRef]]].size should be (1)
  }

  "getQuestions " should " return the question data from the hierarchy of the Question Set " in {
    val data = new ObjectData("do_123", Map[String, AnyRef] ("name" -> "QS1", "objectType" -> "QuestionSet"), Some(Map()), Some(Map[String, AnyRef]("identifier" -> "do_123", "children" -> List(Map[String, AnyRef]("identifier" -> "do_113188615625731", "objectType"->"Question", "visibility"-> "Parent")))))
    val result =  new TestQuestionSetPublisher().getQuestions(data, questionReaderConfig)
    result.size should be(1)
  }

  "getExtDatas " should " return the External data of the provided identifiers " in {
    val result =  new TestQuestionSetPublisher().getExtDatas(List("do_113188615625731", "do_113188615625731.img"), questionReaderConfig)
    result.getOrElse(Map()).size should be(1)
  }

  "deleteExternalData with valid ObjectData" should "should do nothing" in {
    new TestQuestionSetPublisher().deleteExternalData(new ObjectData("do_123", Map()), readerConfig)
  }

  "deleteExternalData " should "do nothing " in {
    val objData = new ObjectData("do_113188615625731", Map());
    new TestQuestionSetPublisher().deleteExternalData(objData, readerConfig)
  }

  "getExtData " should " return ObjectExtData having hierarchy" in {
    val identifier = "do_321"
    val result =  new TestQuestionSetPublisher().getExtData(identifier, 1.0,"", readerConfig)
    result.getOrElse(new ObjectExtData).asInstanceOf[ObjectExtData].hierarchy.getOrElse(Map()).contains("identifier") should be (true)
    result.getOrElse(new ObjectExtData).asInstanceOf[ObjectExtData].hierarchy.getOrElse(Map()).getOrElse("children", List(Map())).asInstanceOf[List[Map[String, AnyRef]]].size should be (1)
  }
  "getHierarchies " should "do nothing " in {
    val identifier = "do_113188615625731";
    new TestQuestionSetPublisher().getHierarchies(List(identifier), readerConfig)
  }
}

class TestQuestionSetPublisher extends QuestionSetPublisher{}
