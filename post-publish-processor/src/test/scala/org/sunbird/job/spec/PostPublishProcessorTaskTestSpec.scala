package org.sunbird.job.spec

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZoneOffset, ZonedDateTime}

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.ArgumentMatchers.any
import org.sunbird.job.functions.PostPublishEventRouter
import org.sunbird.job.task.PostPublishProcessorConfig
import org.sunbird.job.util.{HTTPResponse, HttpUtil, Neo4JUtil}
import org.sunbird.spec.BaseTestSpec
import org.mockito.Mockito
import org.mockito.Mockito._

class PostPublishProcessorTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val strTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: PostPublishProcessorConfig = new PostPublishProcessorConfig(config)

  val mockHttpUtil = mock[HttpUtil]


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }

  "Post Publish Processor" should "search copied contents" in {
    when(mockHttpUtil.post(any[String], any[String])).thenReturn(HTTPResponse(200, """{"id":"api.search-service.search","ver":"3.0","ts":"2020-08-31T22:09:07ZZ","params":{"resmsgid":"bc9a8ac0-f67d-47d5-b093-2077191bf93b","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"count":5,"content":[{"identifier":"do_11301367667942195211854","origin":"do_11300581751853056018","channel":"b00bc992ef25f1a9a8d63291e20efc8d","originData":"{\"name\":\"Origin Content\",\"copyType\":\"deep\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"]}","mimeType":"application/vnd.ekstep.content-collection","contentType":"TextBook","objectType":"Content","status":"Draft","versionKey":"1588583579763"},{"identifier":"do_113005885057662976128","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632481597"},{"identifier":"do_113005885161611264130","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632475439"},{"identifier":"do_113005882957578240124","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632233649"},{"identifier":"do_113005820474007552111","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587624624051"}]}}"""))
    val identifier = "do_11300581751853056018"
    val list = new PostPublishEventRouter(jobConfig, mockHttpUtil).getShallowCopiedContents(identifier)
    list.size should be (4)
    list should contain allOf("do_113005885057662976128", "do_113005885161611264130", "do_113005882957578240124", "do_113005820474007552111")
  }

  it should "return data for a given id" in {
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-mm-dd HH")
    val dateTime = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(dateFormat)
    val utcDateTime = ZonedDateTime.now(ZoneOffset.UTC).format(dateFormat)
    println("DateTime IST: " + dateTime)
    println("DateTime UTC: " + utcDateTime)

  }

}
