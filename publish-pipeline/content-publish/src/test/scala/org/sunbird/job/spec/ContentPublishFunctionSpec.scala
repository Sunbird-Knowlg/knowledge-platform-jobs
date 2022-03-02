package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.junit.Assert.{assertEquals, assertNotEquals, assertNotNull}
import org.mockito.Mockito
import org.sunbird.job.Metrics
import org.sunbird.job.content.function.ContentPublishFunction
import org.sunbird.job.content.publish.domain.Event
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.publish.core.{DefinitionConfig, ObjectData}
import org.sunbird.job.util._
import org.sunbird.spec.BaseTestSpec

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class ContentPublishFunctionSpec extends BaseTestSpec {
  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: ContentPublishConfig = new ContentPublishConfig(config)
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
      put(jobConfig.contentPublishEventCount, new AtomicLong())
      put(jobConfig.contentPublishFailedEventCount, new AtomicLong())
      put(jobConfig.contentPublishSuccessEventCount, new AtomicLong())
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

  "getObjectWithPublishChain" should "should generate publish chain object" in {
    val metadata : Map[String,AnyRef]=  Map("publish_type"-> "public","action"->"publish","iteration"->"1")
    val obj = new ObjectData("123",metadata,null,null)
    val updatedObject = new ContentPublishFunction(jobConfig, mockHttpUtil, mockNeo4jUtil, cassandraUtil, cloudStorageUtil, definitionCache, definitionConfig)(stringTypeInfo).getObjectWithPublishChain(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.PUBLISH_EVENT_WITH_PUBLISH_STRING), 0, 10),obj)
    assertNotNull(updatedObject)
    assertNotEquals(obj,updatedObject)

  }

  "getObjectWithPublishChain" should "should not change object" in {
    val metadata : Map[String,AnyRef]=  Map("publish_type"-> "public","action"->"publish","iteration"->"1")
    val obj = new ObjectData("123",metadata,null,null)
    val updatedObject = new ContentPublishFunction(jobConfig, mockHttpUtil, mockNeo4jUtil, cassandraUtil, cloudStorageUtil, definitionCache, definitionConfig)(stringTypeInfo).getObjectWithPublishChain(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.PUBLISH_CHAIN_EVENT), 0, 10),obj)
    assertNotNull(updatedObject)
    assertEquals(obj,updatedObject)
  }


}


