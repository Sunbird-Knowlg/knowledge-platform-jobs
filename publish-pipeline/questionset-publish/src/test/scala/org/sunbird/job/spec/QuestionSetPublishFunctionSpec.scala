package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.sunbird.job.Metrics
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.core.DefinitionConfig
import org.sunbird.job.questionset.function.QuestionSetPublishFunction
import org.sunbird.job.questionset.publish.domain.PublishMetadata
import org.sunbird.job.questionset.task.QuestionSetPublishConfig
import org.sunbird.job.util._
import org.sunbird.spec.BaseTestSpec

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class QuestionSetPublishFunctionSpec extends BaseTestSpec {
  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: QuestionSetPublishConfig = new QuestionSetPublishConfig(config)
  val mockKafkaClientUtil: KafkaClientUtil = mock[KafkaClientUtil](Mockito.withSettings().serializable())
  var cassandraUtil: CassandraUtil = _
  var definitionCache : DefinitionCache = _
  var definitionConfig : DefinitionConfig = _

  var cloudStorageUtil = new CloudStorageUtil(jobConfig)
  val mockHttpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  val mockNeo4jUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val metrics = Metrics(new ConcurrentHashMap[String, AtomicLong]() {
    {
      put(jobConfig.questionSetPublishEventCount, new AtomicLong())
      put(jobConfig.questionSetPublishSuccessEventCount, new AtomicLong())
      put(jobConfig.questionSetPublishFailedEventCount, new AtomicLong())
    }
  })
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort)
    val session = cassandraUtil.session

    definitionCache = new DefinitionCache()
    definitionConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)


  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => {
      }
    }
  }

  "publishNextEvent" should "throws exception for invalid topic" in {
    val publishChain : List[Map[String,AnyRef]] = List(Map("identifier" -> "do_999","mimeType" -> "application/vnd.sunbird.questionset",
      "objectType" -> "QuestionSet", "lastPublishedBy"->"","pkgVersion"->"2","state"->"Processing"))
    val eData : Map[String,AnyRef]=  Map("publish_type"-> "public","action"->"publishchain","iteration"->"1","publishchain"->publishChain)
    val publishObject : Map[String,AnyRef] = Map("eData" -> eData)
    assertThrows[Exception] {
      val event = new QuestionSetPublishFunction(jobConfig, mockHttpUtil, mockNeo4jUtil, cassandraUtil, cloudStorageUtil, definitionCache, definitionConfig)(stringTypeInfo).publishNextEvent(new PublishMetadata("do_999", "QuestionSet", "application/vnd.sunbird.questionset", 2, "public", JSONUtil.serialize(publishObject)), "")
    }
  }


}


