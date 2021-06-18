package org.sunbird.job.spec.service

import com.datastax.driver.core.Row
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.mockito.{ArgumentCaptor, Mockito}
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.mvcindexer.service.MVCIndexerService
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.util.{CassandraUtil, ElasticSearchUtil, HTTPResponse, HttpUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.{any, endsWith}
import org.mockito.Mockito.{times, verify, when}

import java.util

class MVCProcessorIndexerServiceTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: MVCIndexerConfig = new MVCIndexerConfig(config)
  var cassandraUtil: CassandraUtil = _
  val mockElasticUtil:ElasticSearchUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())
  val mockHttpUtil:HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
//  val mockHttpUtil:HttpUtil = new HttpUtil
  lazy val mvcProcessorIndexer: MVCIndexerService = new MVCIndexerService(jobConfig, mockElasticUtil, mockHttpUtil)

  val updateEsIndexResponse = """{"responseCode":"OK","result":{"content":{"channel":"in.ekstep","framework":"NCF","name":"Ecml bundle Test","language":["English"],"appId":"dev.sunbird.portal","contentEncoding":"gzip","identifier":"do_112806963140329472124","mimeType":"application/vnd.ekstep.ecml-archive","contentType":"Resource","objectType":"Content","artifactUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_112806963140329472124/artifact/1563350021721_do_112806963140329472124.zip","previewUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/ecml/do_112806963140329472124-latest","streamingUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/ecml/do_112806963140329472124-latest","downloadUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_112806963140329472124/ecml-bundle-test_1563350022377_do_112806963140329472124_1.0.ecar","status":"Live","pkgVersion":1,"lastUpdatedOn":"2019-07-17T07:53:25.618+0000"}}}"""

  override protected def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.lmsDbHost, jobConfig.lmsDbPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    testCassandraUtil(cassandraUtil)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
      println("EmbeddedCassandraServerHelper.cleanEmbeddedCassandra() called")
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  "MVCProcessorIndexerService" should "generate es log" in {
    val argumentCaptor = ArgumentCaptor.forClass(classOf[ElasticSearchUtil]).asInstanceOf[ArgumentCaptor[String]]
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1),0, 10)
    when(mockHttpUtil.get(endsWith("content/v3/read/do_112806963140329472124"), any())).thenReturn(HTTPResponse(200, updateEsIndexResponse))

    mvcProcessorIndexer.processMessage(inputEvent)

    val insertedRecord = readFromCassandra(inputEvent.identifier)
    insertedRecord.forEach(col => {
      col.getObject("source") should be ("Sunbird 1")
      col.getObject("sourceurl") should be ("https://dev.sunbirded.org/play/content/do_112806963140329472124")
      col.getObject("level1_name").asInstanceOf[util.ArrayList[String]] should contain("Math-Magic")
      col.getObject("textbook_name").asInstanceOf[util.ArrayList[String]] should contain("How Many Times?")
      col.getObject("level1_concept").asInstanceOf[util.ArrayList[String]] should contain("Addition")
    })

    verify(mockElasticUtil, times(1)).addDocumentWithIndex(argumentCaptor.capture(), argumentCaptor.capture(), argumentCaptor.capture());
    val esRecordMap = JSONUtil.deserialize[Map[String, AnyRef]](argumentCaptor.getAllValues.get(0))

    esRecordMap("level1Concept").asInstanceOf[List[String]] should contain("Addition")
    esRecordMap("artifactUrl") should be("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_112806963140329472124/artifact/1563350021721_do_112806963140329472124.zip")
    esRecordMap("downloadUrl") should be("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_112806963140329472124/ecml-bundle-test_1563350022377_do_112806963140329472124_1.0.ecar")
    esRecordMap("pkgVersion") should be(1)
    esRecordMap("previewUrl") should be("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/ecml/do_112806963140329472124-latest")
    esRecordMap("level1Name").asInstanceOf[List[String]] should contain("Math-Magic")
    esRecordMap("identifier") should be("do_112806963140329472124")
    esRecordMap("appId") should be("dev.sunbird.portal")
    esRecordMap("objectType") should be("Content")
    esRecordMap("name") should be("Ecml bundle Test")
    esRecordMap("lastUpdatedOn") should be("2019-07-17T07:53:25.618+0000")
    esRecordMap("status") should be("Live")
    esRecordMap("source").asInstanceOf[List[String]] should contain("Sunbird 1")
    esRecordMap("contentEncoding") should be("gzip")
    esRecordMap("contentType") should be("Resource")
    esRecordMap("channel") should be("in.ekstep")
    esRecordMap("textbook_name").asInstanceOf[List[String]] should contain("How Many Times?")
    esRecordMap("mimeType") should be("application/vnd.ekstep.ecml-archive")
    esRecordMap("language").asInstanceOf[List[String]] should contain("English")
    esRecordMap("sourceURL") should be("https://dev.sunbirded.org/play/content/do_112806963140329472124")
    argumentCaptor.getAllValues.get(2) should be(inputEvent.identifier)
  }

  "MVCProcessorIndexerService" should "update content rating" in {
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1), 0, 11)
  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }

  def readFromCassandra(contentId: String): util.List[Row] = {
    val query = s"select * from ${jobConfig.dbKeyspace}.${jobConfig.dbTable} where content_id='${contentId}' ALLOW FILTERING;"
    cassandraUtil.find(query)
  }
}
