package org.sunbird.job.publish.helpers.spec

import org.mockito.Mockito.when
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.publish.helpers.QuestionSetPublisher
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}

class QuestionSetPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit val mockCassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  implicit val mockExtDataConfig: ExtDataConfig = mock[ExtDataConfig](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    super.beforeAll()
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
    when(mockCassandraUtil.upsert(ArgumentMatchers.anyString())).thenReturn(true)
    new TestQuestionSetPublisher().saveExternalData(data, mockExtDataConfig)
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
    val data = new ObjectData("do_123", Map[String, AnyRef] ("name" -> "QS1"), Some(Map()), Some(Map[String, AnyRef]("identifier" -> "do_123", "children" -> List(Map[String, AnyRef]("identifier" -> "do_124", "objectType"->"QuestionSet", "visibility"-> "Parent")))))
    val result: Option[ObjectData] = new TestQuestionSetPublisher().enrichObjectMetadata(data)
    result.getOrElse(new ObjectData("do_123", Map())).pkgVersion should be (1.asInstanceOf[Number])
    result.size should be (1)
  }

  "deleteExternalData with valid ObjectData" should "should do nothing" in {
    new TestQuestionSetPublisher().deleteExternalData(new ObjectData("do_123", Map()), mockExtDataConfig)
  }


}

class TestQuestionSetPublisher extends QuestionSetPublisher{}