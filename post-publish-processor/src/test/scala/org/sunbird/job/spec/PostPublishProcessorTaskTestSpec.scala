package org.sunbird.job.spec

import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
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
import org.sunbird.job.functions.DIALCodeLinkFunction
import org.mockito.ArgumentMatchers.{any, anyString, endsWith}
import org.sunbird.job.functions.PostPublishEventRouter
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, Neo4JUtil}
import org.sunbird.spec.BaseTestSpec
import org.mockito.Mockito
import org.mockito.Mockito._
import org.neo4j.driver.v1.StatementResult
import org.sunbird.job.postpublish.domain.Event
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.task.{PostPublishProcessorConfig, PostPublishProcessorStreamTask}

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
  val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  var cassandraUtil: CassandraUtil = _
  // For Shallow Copy and Batch Creation
  val startDate = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val searchRequestBody = s"""{"request":{"filters":{"status":["Draft","Review","Live","Unlisted","Failed"],"origin":"do_11300581751853056018"},"fields":["identifier","mimeType","contentType","versionKey","channel","status","pkgVersion","lastPublishedBy","origin","originData"]}}"""
  val batchRequestBody = s"""{"request":{"createdFor":["ORG_001"],"createdBy":"874ed8a5-782e-4f6c-8f36-e0288455901e","name":"Origin Content","enrollmentType":"open","courseId":"do_11300581751853056018","startDate":"${startDate}"}}"""
  // END
  // For Dial Codes
  val searchRequestForQRImageEvent1 = s"""{"request":{"filters":{"status":["Draft","Review","Live","Unlisted","Failed"],"origin":"do_113214556543234"},"fields":["identifier","mimeType","contentType","versionKey","channel","status","pkgVersion","lastPublishedBy","origin","originData"]}}"""
  val searchRequestForQRImageEvent2 = s"""{"request":{"filters":{"status":["Draft","Review","Live","Unlisted","Failed"],"origin":"do_113214556543235"},"fields":["identifier","mimeType","contentType","versionKey","channel","status","pkgVersion","lastPublishedBy","origin","originData"]}}"""
  val searchRequestForQRImageEvent3 = s"""{"request":{"filters":{"status":["Draft","Review","Live","Unlisted","Failed"],"origin":"do_113214556543236"},"fields":["identifier","mimeType","contentType","versionKey","channel","status","pkgVersion","lastPublishedBy","origin","originData"]}}"""
  val searchRequestForQRImageEvent4 = s"""{"request":{"filters":{"status":["Draft","Review","Live","Unlisted","Failed"],"origin":"do_113214556543237"},"fields":["identifier","mimeType","contentType","versionKey","channel","status","pkgVersion","lastPublishedBy","origin","originData"]}}"""
  val qrRequestUrl = "https://localhost/action/content/v3/dialcode/reserve/do_113214556543237"
  val qrRequestBody = s"""{"request":{"dialcodes":{"count":1,"qrCodeSpec":{"errorCorrectionLevel":"H"}}}}"""
  val qrRequestHeaders: Map[String, String] = Map[String, String]("X-Channel-Id" -> "b00bc992ef25f1a9a8d63291e20efc8d",
    "Content-Type" -> "application/json")
  // END
  val gson = new Gson()

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
    when(mockHttpUtil.post(endsWith("/v3/search"), anyString(), any())).thenReturn(HTTPResponse(200, """{"id":"api.search-service.search","ver":"3.0","ts":"2020-08-31T22:09:07ZZ","params":{"resmsgid":"bc9a8ac0-f67d-47d5-b093-2077191bf93b","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"count":5,"content":[{"identifier":"do_11301367667942195211854","origin":"do_11300581751853056018","channel":"b00bc992ef25f1a9a8d63291e20efc8d","originData":"{\"name\":\"Origin Content\",\"copyType\":\"deep\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"]}","mimeType":"application/vnd.ekstep.content-collection","contentType":"TextBook","objectType":"Content","status":"Draft","versionKey":"1588583579763"},{"identifier":"do_113005885057662976128","origin":"do_11300581751853056018","pkgVersion": 2,"channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632481597"},{"identifier":"do_113005885161611264130","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632475439"},{"identifier":"do_113005882957578240124","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632233649"},{"identifier":"do_113005820474007552111","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587624624051"}]}}"""))
    val identifier = "do_11300581751853056018"
    val list = new PostPublishEventRouter(jobConfig, mockHttpUtil).getShallowCopiedContents(identifier)
    list.size should be(4)
    list.map(c => c.identifier) should contain allOf("do_113005885057662976128", "do_113005885161611264130", "do_113005882957578240124", "do_113005820474007552111")
  }

  "Post Publish Processor" should "process and return the metadata for batch " in {
    val metaData = new java.util.HashMap[String, AnyRef]() {
      {
        put("IL_UNIQUE_ID", "do_11300581751853056018")
        put("identifier", "do_11300581751853056018")
        put("name", "Origin Content")
        put("createdBy", "874ed8a5-782e-4f6c-8f36-e0288455901e")
        put("channel", "b00bc992ef25f1a9a8d63291e20efc8d")
        put("trackable", "{\"enabled\":\"Yes\",\"autoBatch\":\"Yes\"}")
        put("createdFor", util.Arrays.asList("ORG_001"))
      }
    }

    when(mockNeo4JUtil.getNodeProperties(anyString())).thenReturn(metaData)
    val identifier = "do_11300581751853056018"
    val batchMetadata = new PostPublishEventRouter(jobConfig, mockHttpUtil, mockNeo4JUtil, cassandraUtil).getBatchDetails(identifier)(mockNeo4JUtil, cassandraUtil, jobConfig)
    batchMetadata.size() should be(4)
    batchMetadata.get("identifier") should be("do_11300581751853056018")
  }

  "Post Publish Processor" should "process and return the empty metadata for batch" in {
    val metaData = new java.util.HashMap[String, AnyRef]() {
      {
        put("IL_UNIQUE_ID", "do_11300581751853056018")
        put("identifier", "do_11300581751853056018")
        put("name", "Origin Content")
        put("createdBy", "874ed8a5-782e-4f6c-8f36-e0288455901e")
        put("channel", "b00bc992ef25f1a9a8d63291e20efc8d")
        put("trackable", "{\"enabled\":\"false\",\"autoBatch\":\"false\"}")
        put("createdFor", util.Arrays.asList("ORG_001"))
      }
    }

    when(mockNeo4JUtil.getNodeProperties(anyString())).thenReturn(metaData)
    val identifier = "do_11300581751853056018"
    val batchMetadata = new PostPublishEventRouter(jobConfig, mockHttpUtil, mockNeo4JUtil, cassandraUtil).getBatchDetails(identifier)(mockNeo4JUtil, cassandraUtil, jobConfig)
    batchMetadata.isEmpty() should be(true)
  }

  "Post Publish Processor" should "process and return the metadata for dialcode generation" in {
    val metadata = new java.util.HashMap[String, AnyRef]() {
      {
        put("IL_UNIQUE_ID", "do_113214556543234")
        put("identifier", "do_113214556543234")
        put("name", "Origin Content")
        put("createdBy", "874ed8a5-782e-4f6c-8f36-e0288455901e")
        put("channel", "b00bc992ef25f1a9a8d63291e20efc8d")
        put("trackable", "{\"enabled\":\"No\",\"autoBatch\":\"No\"}")
        put("createdFor", util.Arrays.asList("ORG_001"))
        put("reservedDialcodes", "{\"Q1I5I3\": 0}")
        put("dialcodes", util.Arrays.asList("Q1I5I3"))
        put("primaryCategory", "Course")
      }
    }

    val identifier = "do_113214556543234"
    when(mockNeo4JUtil.getNodeProperties(anyString())).thenReturn(metadata)
    val qrEventMap1 = gson.fromJson(EventFixture.QREVENT_1, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]].asScala ++ Map("partition" -> 0.asInstanceOf[Any])
    val event = new Event(qrEventMap1.asJava,0, 10)
    val dialcodeMetadata = new PostPublishEventRouter(jobConfig, mockHttpUtil, mockNeo4JUtil, cassandraUtil).getDialCodeDetails(identifier, event)(mockNeo4JUtil, jobConfig)
    dialcodeMetadata.isEmpty() should be(false)
    dialcodeMetadata.get("dialcodes").asInstanceOf[util.List[String]] should contain("Q1I5I3")
  }

  "Post Publish Processor" should "process and return the empty metadata for dialcode generation" in {
    val metadata = new java.util.HashMap[String, AnyRef]() {
      {
        put("IL_UNIQUE_ID", "do_113214556543234")
        put("identifier", "do_113214556543234")
        put("name", "Origin Content")
        put("createdBy", "874ed8a5-782e-4f6c-8f36-e0288455901e")
        put("channel", "b00bc992ef25f1a9a8d63291e20efc8d")
        put("trackable", "{\"enabled\":\"No\",\"autoBatch\":\"No\"}")
        put("createdFor", util.Arrays.asList("ORG_001"))
        put("reservedDialcodes", "{\"Q1I5I3\": 0}")
        put("dialcodes", util.Arrays.asList("Q1I5I3"))
        put("primaryCategory", "Textbook")
      }
    }

    val identifier = "do_113214556543234"
    when(mockNeo4JUtil.getNodeProperties(anyString())).thenReturn(metadata)
    val qrEventMap1 = gson.fromJson(EventFixture.QREVENT_1, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]].asScala ++ Map("partition" -> 0.asInstanceOf[Any])
    val event = new Event(qrEventMap1.asJava, 0, 11)
    val dialcodeMetadata = new PostPublishEventRouter(jobConfig, mockHttpUtil, mockNeo4JUtil, cassandraUtil).getDialCodeDetails(identifier, event)(mockNeo4JUtil, jobConfig)
    dialcodeMetadata.isEmpty() should be(true)
  }


  "Post Publish Processor" should "process request for dialcode generation return the nothing for QR Image " in {
    val edata = new java.util.HashMap[String, AnyRef]() {
      {
        put("IL_UNIQUE_ID", "do_113214556543234")
        put("identifier", "do_113214556543234")
        put("name", "Origin Content")
        put("createdBy", "874ed8a5-782e-4f6c-8f36-e0288455901e")
        put("channel", "b00bc992ef25f1a9a8d63291e20efc8d")
        put("trackable", "{\"enabled\":\"No\",\"autoBatch\":\"No\"}")
        put("createdFor", util.Arrays.asList("ORG_001"))
        put("reservedDialcodes", "{\"Q1I5I3\": 0}")
        put("dialcodes", util.Arrays.asList("Q1I5I3"))
        put("primaryCategory", "Course")
      }
    }
    val dialcode = new DIALCodeLinkFunction(jobConfig, mockHttpUtil, mockNeo4JUtil, cassandraUtil).getDialcode(edata)
    dialcode should be("")
  }

  "Post Publish Processor" should "process request for dialcode generation and return the dialcode for QR Image " in {
    val edata = new java.util.HashMap[String, AnyRef]() {
      {
        put("IL_UNIQUE_ID", "do_113214556543235")
        put("identifier", "do_113214556543235")
        put("name", "Origin Content")
        put("createdBy", "874ed8a5-782e-4f6c-8f36-e0288455901e")
        put("channel", "b00bc992ef25f1a9a8d63291e20efc8d")
        put("trackable", "{\"enabled\":\"No\",\"autoBatch\":\"No\"}")
        put("createdFor", util.Arrays.asList("ORG_001"))
        put("reservedDialcodes", "{\"Q1I5I4\": 0}")
        put("dialcodes", util.Arrays.asList("Q1I5I4"))
        put("primaryCategory", "Course")
      }
    }
    val dialcode = new DIALCodeLinkFunction(jobConfig, mockHttpUtil, mockNeo4JUtil, cassandraUtil).getDialcode(edata)
    dialcode should be("Q1I5I4")
  }

  "Post Publish Processor" should " use the existing reserved dialcode and return that dialcode for QR Image " in {
    val edata = new java.util.HashMap[String, AnyRef]() {
      {
        put("IL_UNIQUE_ID", "do_113214556543236")
        put("identifier", "do_113214556543236")
        put("name", "Origin Content")
        put("createdBy", "874ed8a5-782e-4f6c-8f36-e0288455901e")
        put("channel", "b00bc992ef25f1a9a8d63291e20efc8d")
        put("trackable", "{\"enabled\":\"No\",\"autoBatch\":\"No\"}")
        put("createdFor", util.Arrays.asList("ORG_001"))
        put("reservedDialcodes", "{\"Q1I5I5\": 0}")
        put("primaryCategory", "Course")
      }
    }
    doNothing().when(mockNeo4JUtil).updateNodeProperty(anyString, anyString, anyString)
    val dialcode = new DIALCodeLinkFunction(jobConfig, mockHttpUtil, mockNeo4JUtil, cassandraUtil).getDialcode(edata)
    dialcode should be("Q1I5I5")
  }


  "Post Publish Processor" should " reserve a dialcode and return that dialcode for QR Image " in {
    val edata = new java.util.HashMap[String, AnyRef]() {
      {
        put("IL_UNIQUE_ID", "do_113214556543237")
        put("identifier", "do_113214556543237")
        put("name", "Origin Content")
        put("createdBy", "874ed8a5-782e-4f6c-8f36-e0288455901e")
        put("channel", "b00bc992ef25f1a9a8d63291e20efc8d")
        put("trackable", "{\"enabled\":\"No\",\"autoBatch\":\"No\"}")
        put("createdFor", util.Arrays.asList("ORG_001"))
        put("primaryCategory", "Course")
      }
    }
    doNothing().when(mockNeo4JUtil).updateNodeProperty(anyString, anyString, anyString)
    when(mockHttpUtil.post(anyString, anyString, any())).thenReturn(HTTPResponse(200, """{"result": {"reservedDialcodes": {"Q2I5I9" : 0}}}"""))

    val dialcode = new DIALCodeLinkFunction(jobConfig, mockHttpUtil, mockNeo4JUtil, cassandraUtil).getDialcode(edata)
    dialcode should be("Q2I5I9")
  }

  ignore should "run all the scenarios for a given event" in {
    when(mockHttpUtil.post(jobConfig.searchAPIPath, searchRequestBody)).thenReturn(HTTPResponse(200, """{"id":"api.search-service.search","ver":"3.0","ts":"2020-08-31T22:09:07ZZ","params":{"resmsgid":"bc9a8ac0-f67d-47d5-b093-2077191bf93b","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"count":5,"content":[{"identifier":"do_11301367667942195211854","origin":"do_11300581751853056018","channel":"b00bc992ef25f1a9a8d63291e20efc8d","originData":"{\"name\":\"Origin Content\",\"copyType\":\"deep\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"]}","mimeType":"application/vnd.ekstep.content-collection","contentType":"TextBook","objectType":"Content","status":"Draft","versionKey":"1588583579763"},{"identifier":"do_113005885057662976128","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632481597"},{"identifier":"do_113005885161611264130","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632475439"},{"identifier":"do_113005882957578240124","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632233649"},{"identifier":"do_113005820474007552111","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587624624051"}]}}"""))
    when(mockHttpUtil.post(jobConfig.batchCreateAPIPath, batchRequestBody)).thenReturn(HTTPResponse(200, """{}"""))
    when(mockKafkaUtil.kafkaStringSink(jobConfig.contentPublishTopic)).thenReturn(new PublishEventSink)
    when(mockHttpUtil.post(qrRequestUrl, qrRequestBody, qrRequestHeaders)).thenReturn(HTTPResponse(200, """{"result": {"reservedDialcodes": {"Q2I5I9" : 0}}}"""))
    when(mockHttpUtil.post(jobConfig.searchAPIPath, searchRequestForQRImageEvent1)).thenReturn(HTTPResponse(200, """{"responseCode": "OK","result": {"count": 5,"content": []}}"""))
    when(mockHttpUtil.post(jobConfig.searchAPIPath, searchRequestForQRImageEvent2)).thenReturn(HTTPResponse(200, """{"responseCode": "OK","result": {"count": 5,"content": []}}"""))
    when(mockHttpUtil.post(jobConfig.searchAPIPath, searchRequestForQRImageEvent3)).thenReturn(HTTPResponse(200, """{"responseCode": "OK","result": {"count": 5,"content": []}}"""))
    when(mockHttpUtil.post(jobConfig.searchAPIPath, searchRequestForQRImageEvent4)).thenReturn(HTTPResponse(200, """{"responseCode": "OK","result": {"count": 5,"content": []}}"""))
    when(mockKafkaUtil.kafkaStringSink(jobConfig.QRImageGeneratorTopic)).thenReturn(new QRImageEventSink)
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new PostPublishEventSource)
    when(mockHttpUtil.post(endsWith("/v3/search"), anyString(), any())).thenReturn(HTTPResponse(200, """{"id":"api.search-service.search","ver":"3.0","ts":"2020-08-31T22:09:07ZZ","params":{"resmsgid":"bc9a8ac0-f67d-47d5-b093-2077191bf93b","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"count":5,"content":[{"identifier":"do_11301367667942195211854","origin":"do_11300581751853056018","channel":"b00bc992ef25f1a9a8d63291e20efc8d","originData":"{\"name\":\"Origin Content\",\"copyType\":\"deep\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"]}","mimeType":"application/vnd.ekstep.content-collection","contentType":"TextBook","objectType":"Content","status":"Draft","versionKey":"1588583579763"},{"identifier":"do_113005885057662976128","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632481597"},{"identifier":"do_113005885161611264130","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632475439"},{"identifier":"do_113005882957578240124","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632233649"},{"identifier":"do_113005820474007552111","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587624624051"}]}}"""))
    when(mockHttpUtil.post(endsWith("/private/v1/course/batch/create"), anyString(), any())).thenReturn(HTTPResponse(200, """{}"""))
    val trackable = """{"enabled":"Yes","autoBatch":"Yes"}"""
    when(mockNeo4JUtil.getNodeProperties("do_11300581751853056018")).thenReturn(Map("name" -> "Origin Content", "createdBy" -> "874ed8a5-782e-4f6c-8f36-e0288455901e", "createdFor" -> util.Arrays.asList("ORG_001"), "channel" -> "b00bc992ef25f1a9a8d63291e20efc8d", "trackable" -> trackable).asJava)
    val mockResult = mock[StatementResult](Mockito.withSettings().serializable())
    when(mockNeo4JUtil.executeQuery(anyString())).thenReturn(mockResult)
    val streamTask = new PostPublishProcessorStreamTask(jobConfig, mockKafkaUtil, mockHttpUtil)
    streamTask.process()

    PublishEventSink.values.size() should be(4)
    PublishEventSink.values.forEach(event => {
      println("PUBLISH_EVENT: " + event)
    })

    QRImageEventSink.values.size() should be(3)
    QRImageEventSink.values.forEach(event => {
      println("QR_IMAGE_EVENT: " + event)
    })
  }
}

class PostPublishEventSource extends SourceFunction[Event] {
  override def run(ctx: SourceContext[Event]): Unit = {
    val gson = new Gson()
    // Event for Batch Creation and ShallowCopy
    val eventMap1 = gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]].asScala ++ Map("partition" -> 0.asInstanceOf[Any])
    ctx.collect(new Event(eventMap1.asJava,0, 10))
    // Event for Dial Codes
    val qrEventMap1 = gson.fromJson(EventFixture.QREVENT_1, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]].asScala ++ Map("partition" -> 0.asInstanceOf[Any])
    ctx.collect(new Event(qrEventMap1.asJava,0, 11))
    val qrEventMap2 = gson.fromJson(EventFixture.QREVENT_2, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]].asScala ++ Map("partition" -> 0.asInstanceOf[Any])
    ctx.collect(new Event(qrEventMap2.asJava,0, 12))
    val qrEventMap3 = gson.fromJson(EventFixture.QREVENT_3, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]].asScala ++ Map("partition" -> 0.asInstanceOf[Any])
    ctx.collect(new Event(qrEventMap3.asJava,0, 13))
    val qrEventMap4 = gson.fromJson(EventFixture.QREVENT_4, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]].asScala ++ Map("partition" -> 0.asInstanceOf[Any])
    ctx.collect(new Event(qrEventMap4.asJava,0, 14))
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

class QRImageEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      QRImageEventSink.values.add(value)
    }
  }
}

object QRImageEventSink {
  val values: util.List[String] = new util.ArrayList()
}
