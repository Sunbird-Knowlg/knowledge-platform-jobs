package org.sunbird.job.publish.util.spec

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData}
import org.sunbird.job.publish.util.CloudStorageUtil
import org.sunbird.job.questionset.publish.util.QuestionPublishUtil
import org.sunbird.job.questionset.task.QuestionSetPublishConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}

import scala.collection.JavaConverters._

class QuestionPublishUtilSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val ec = ExecutionContexts.global
  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: QuestionSetPublishConfig = new QuestionSetPublishConfig(config)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.questionKeyspaceName, jobConfig.questionTableName)
  implicit val cloudStorageUtil = new CloudStorageUtil(jobConfig)
  implicit val defCache = new DefinitionCache()
  implicit val defConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)
  implicit val publishConfig: PublishConfig = new PublishConfig(config, "")
  implicit val httpUtil = new HttpUtil

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

  def delay(time: Long): Unit = {
    try {
      Thread.sleep(time)
    } catch {
      case ex: Exception => print("")
    }
  }

  "publishQuestions " should " publish questions in questionset " in {
    when(mockNeo4JUtil.getNodeProperties("do_113188615625731")).thenReturn(Map[String, AnyRef]("identifier" -> "do_113188615625731", "objectType" -> "Question", "IL_FUNC_OBJECT_TYPE" -> "Question", "visibility"-> "Parent", "mimeType" -> "application/vnd.sunbird.question", "primaryCategory" -> "some category", "name" -> "Some Question", "code" -> "some code").asJava)

    val identifier = "do_123";
    val obj1: ObjectData = new ObjectData("do_113188615625731", Map[String, AnyRef]("identifier" -> "do_113188615625731", "objectType"->"Question", "visibility"-> "Parent", "mimeType" -> "application/vnd.sunbird.question", "primaryCategory" -> "some category", "name" -> "Some Question", "code" -> "some code"))
    val objList: List[ObjectData] = List(obj1)
    QuestionPublishUtil.publishQuestions(identifier, objList)(ec, mockNeo4JUtil, cassandraUtil, readerConfig, cloudStorageUtil, defCache, defConfig, publishConfig, httpUtil)
  }

}
