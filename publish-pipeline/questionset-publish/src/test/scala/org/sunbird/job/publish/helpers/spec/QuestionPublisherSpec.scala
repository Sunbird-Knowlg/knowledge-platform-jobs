package org.sunbird.job.publish.helpers.spec

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.publish.helpers.QuestionPublisher
import org.sunbird.job.task.QuestionSetPublishConfig
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}
import org.sunbird.publish.core.{ExtDataConfig, ObjectData}


class QuestionPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

	implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
//	implicit val mockCassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
//	implicit val mockExtDataConfig: ExtDataConfig = mock[ExtDataConfig](Mockito.withSettings().serializable())
	implicit var cassandraUtil: CassandraUtil = _
	val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
	val jobConfig: QuestionSetPublishConfig = new QuestionSetPublishConfig(config)
	implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.questionKeyspaceName, jobConfig.questionTableName)

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

	"enrichObjectMetadata" should "enrich the Question pkgVersion metadata" in {
		val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]))
		val result: ObjectData = new TestQuestionPublisher().enrichObjectMetadata(data).getOrElse(data)
		result.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number] should be(1.0.asInstanceOf[Number])
	}
	"validateQuestion with invalid external data" should "return exception messages" in {
		val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]), Some(Map[String, AnyRef]("body" -> "body")))
		val result: List[String] = new TestQuestionPublisher().validateQuestion(data, data.identifier)
		result.size should be(2)
	}

	"validateQuestion with external data having interaction" should "validate the Question external data" in {
		val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "interactionTypes" -> new util.ArrayList[String]() {add("choice")}), Some(Map[String, AnyRef]("body" -> "body", "answer" -> "answer")))
		val result: List[String] = new TestQuestionPublisher().validateQuestion(data, data.identifier)
		result.size should be(3)
	}

	"saveExternalData " should "save external data to cassandra table" in {
		val data = new ObjectData("do_123", Map[String, AnyRef](), Some(Map[String, AnyRef]("body" -> "body", "answer" -> "answer")))
//		when(mockCassandraUtil.upsert(ArgumentMatchers.anyString())).thenReturn(true)
		new TestQuestionPublisher().saveExternalData(data, readerConfig)
	}

  "getExtData " should "return the external data for the identifier " in {
    val identifier = "do_113188615625731";
    val result: Option[Map[String, AnyRef]] = new TestQuestionPublisher().getExtData(identifier, 0.0, readerConfig)
		result.getOrElse(Map()).size should be(6)
  }

	"getExtData " should "return the external data for the image identifier " in {
		val identifier = "do_113188615625731";
		val result: Option[Map[String, AnyRef]] = new TestQuestionPublisher().getExtData(identifier, 1.0, readerConfig)
		result.getOrElse(Map()).size should be(7)
	}

	"getHierarchy " should "do nothing " in {
		val identifier = "do_113188615625731";
		new TestQuestionPublisher().getExtData(identifier, 1.0, readerConfig)
	}

	"getExtDatas " should "do nothing " in {
		val identifier = "do_113188615625731";
		new TestQuestionPublisher().getExtDatas(List(identifier), readerConfig)
	}

	"getHierarchies " should "do nothing " in {
		val identifier = "do_113188615625731";
		new TestQuestionPublisher().getHierarchies(List(identifier), readerConfig)
	}

	def delay(time: Long): Unit = {
		try {
			Thread.sleep(time)
		} catch {
			case ex: Exception => print("")
		}
	}
}

class TestQuestionPublisher extends QuestionPublisher {}
