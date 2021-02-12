package org.sunbird.job.publish.helpers.spec

import java.util

import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.publish.helpers.QuestionPublisher
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}


class QuestionPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

	implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
	implicit val mockCassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
	implicit val mockExtDataConfig: ExtDataConfig = mock[ExtDataConfig](Mockito.withSettings().serializable())

	override protected def beforeAll(): Unit = {
		super.beforeAll()
	}

	override protected def afterAll(): Unit = {
		super.afterAll()
	}

	"enrichObjectMetadata" should "enrich the Question pkgVersion metadata" in {
		val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]))
		val obj = new TestQuestionPublisher()
		val result: ObjectData = obj.enrichObjectMetadata(data).getOrElse(data)
		result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number] should be(1.0.asInstanceOf[Number])
	}
	"validateQuestion with invalid external data" should "return exception messages" in {
		val metadata: Map[String, AnyRef] = Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef])
		val extData: Map[String, AnyRef] = Map[String, AnyRef]("body" -> "body")
		val data = new ObjectData("do_123", metadata, Some(extData))
		val obj = new TestQuestionPublisher()
		val result: List[String] = obj.validateQuestion(data, data.identifier)
		result.size should be(2)
	}

	"validateQuestion with external data having interaction" should "validate the Question external data" in {
		val metadata: Map[String, AnyRef] = Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "interactionTypes" -> new util.ArrayList[String]() {add("choice")})
		val extData: Map[String, AnyRef] = Map[String, AnyRef]("body" -> "body", "answer" -> "answer")
		val data = new ObjectData("do_123", metadata, Some(extData))
		val result: List[String] = new TestQuestionPublisher().validateQuestion(data, data.identifier)
		result.size should be(3)
	}

}

class TestQuestionPublisher extends QuestionPublisher {}
