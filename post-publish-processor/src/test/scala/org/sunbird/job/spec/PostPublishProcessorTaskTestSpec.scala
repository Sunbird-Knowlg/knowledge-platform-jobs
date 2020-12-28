package org.sunbird.job.spec

import java.util
import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.{anyString, endsWith}
import org.sunbird.job.functions.PostPublishEventRouter
import org.sunbird.job.task.{PostPublishProcessorConfig, PostPublishProcessorStreamTask}
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, Neo4JUtil}
import org.sunbird.spec.BaseTestSpec
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.job.postpublish.domain.Event
//import org.neo4j.graphdb.GraphDatabaseService
//import org.neo4j.graphdb.factory.GraphDatabaseFactory
//import org.neo4j.kernel.configuration.BoltConnector
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture

import scala.collection.JavaConverters._

class PostPublishProcessorTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val strTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  implicit val jobConfig: PostPublishProcessorConfig = new PostPublishProcessorConfig(config)

  implicit val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil]
  var cassandraUtil: CassandraUtil = _




  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
    val session = cassandraUtil.session

    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => {
      }
    }
    flinkCluster.after()
  }



  "Post Publish Processor" should "process and find shallow copied contents" in {
    when(mockHttpUtil.post(endsWith("/v3/search"), anyString())).thenReturn(HTTPResponse(200, """{"id":"api.search-service.search","ver":"3.0","ts":"2020-08-31T22:09:07ZZ","params":{"resmsgid":"bc9a8ac0-f67d-47d5-b093-2077191bf93b","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"count":5,"content":[{"identifier":"do_11301367667942195211854","origin":"do_11300581751853056018","channel":"b00bc992ef25f1a9a8d63291e20efc8d","originData":"{\"name\":\"Origin Content\",\"copyType\":\"deep\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"]}","mimeType":"application/vnd.ekstep.content-collection","contentType":"TextBook","objectType":"Content","status":"Draft","versionKey":"1588583579763"},{"identifier":"do_113005885057662976128","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632481597"},{"identifier":"do_113005885161611264130","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632475439"},{"identifier":"do_113005882957578240124","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632233649"},{"identifier":"do_113005820474007552111","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587624624051"}]}}"""))
    val identifier = "do_11300581751853056018"
    val list = new PostPublishEventRouter(jobConfig, mockHttpUtil).getShallowCopiedContents(identifier)
    list.size should be (4)
    list.map(c => c.identifier) should contain allOf("do_113005885057662976128", "do_113005885161611264130", "do_113005882957578240124", "do_113005820474007552111")
  }

  ignore should "run all the scenarios for a given event" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new PostPublishEventSource)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.contentPublishTopic)).thenReturn(new PublishEventSink)
    when(mockHttpUtil.post(endsWith("/v3/search"), anyString())).thenReturn(HTTPResponse(200, """{"id":"api.search-service.search","ver":"3.0","ts":"2020-08-31T22:09:07ZZ","params":{"resmsgid":"bc9a8ac0-f67d-47d5-b093-2077191bf93b","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"count":5,"content":[{"identifier":"do_11301367667942195211854","origin":"do_11300581751853056018","channel":"b00bc992ef25f1a9a8d63291e20efc8d","originData":"{\"name\":\"Origin Content\",\"copyType\":\"deep\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"]}","mimeType":"application/vnd.ekstep.content-collection","contentType":"TextBook","objectType":"Content","status":"Draft","versionKey":"1588583579763"},{"identifier":"do_113005885057662976128","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632481597"},{"identifier":"do_113005885161611264130","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632475439"},{"identifier":"do_113005882957578240124","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632233649"},{"identifier":"do_113005820474007552111","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587624624051"}]}}"""))
    when(mockHttpUtil.post(endsWith("/private/v1/course/batch/create"), anyString)).thenReturn(HTTPResponse(200, """{}"""))
    val trackable = """{"enabled":"Yes","autoBatch":"Yes"}"""
    when(mockNeo4JUtil.getNodeProperties("do_11300581751853056018")).thenReturn(Map("name" -> "Origin Content", "createdBy" -> "874ed8a5-782e-4f6c-8f36-e0288455901e", "createdFor" -> util.Arrays.asList("ORG_001"), "channel" -> "b00bc992ef25f1a9a8d63291e20efc8d", "trackable" -> trackable).asJava)
    val streamTask = new PostPublishProcessorStreamTask(jobConfig, mockKafkaUtil, mockHttpUtil)
    streamTask.process()

    PublishEventSink.values.size() should be(4)
    PublishEventSink.values.forEach(event => {
      println("PUBLISH_EVENT: " + event)
    })
  }
}

class PostPublishEventSource extends SourceFunction[Event] {
  override def run(ctx: SourceContext[Event]): Unit = {
    val gson = new Gson()
    val eventMap1 = gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]].asScala ++ Map("partition" -> 0.asInstanceOf[Any])
    ctx.collect(new Event(eventMap1.asJava))
  }

  override def cancel() = {}
}

class PublishEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      PublishEventSink.values.add(value)
    }
  }
}

object PublishEventSink {
  val values: util.List[String] = new util.ArrayList()
}
