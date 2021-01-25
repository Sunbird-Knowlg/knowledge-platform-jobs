package org.sunbird.job.spec.service

import java.util

import com.datastax.driver.core.Row
import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.joda.time.DateTimeUtils
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.service.VideoStreamService
import org.sunbird.job.task.VideoStreamGeneratorConfig
import org.sunbird.spec.BaseTestSpec
import org.json4s.jackson.JsonMethods.parse
import org.sunbird.job.spec.MockAzureWebServer
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil}
import org.sunbird.job.domain.Event

import scala.collection.JavaConverters._

class VideoStreamServiceTestSpec extends BaseTestSpec {
  val gson = new Gson()
  var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf")
  implicit lazy val jobConfig: VideoStreamGeneratorConfig = new VideoStreamGeneratorConfig(config)
  val server = new MockAzureWebServer(jobConfig)
  implicit val httpUtilImplicit: HttpUtil = new HttpUtil

  override protected def beforeAll(): Unit = {
    DateTimeUtils.setCurrentMillisFixed(1605816926271L);
//    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    testCassandraUtil(cassandraUtil)
    super.beforeAll()
//    server.setupRestUtilData()
  }

  override protected def afterAll(): Unit = {
    DateTimeUtils.setCurrentMillisSystem();
    server.close()
    super.afterAll()
  }

  "VideoStreamService" should "process job request" in {
    val videoStreamService = new VideoStreamService();
    videoStreamService.processJobRequest()
  }

  "VideoStreamService" should "submit job request" in {
    val eventMap1 = gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]].asScala ++ Map("partition" -> 0.asInstanceOf[Any])
//    val eventMap1 = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1))

    val videoStreamService = new VideoStreamService();
    videoStreamService.submitJobRequest(new Event(eventMap1.asJava).eData)

    val event1Progress = readFromCassandra(EventFixture.EVENT_1)
    event1Progress.size() should be(1)

    event1Progress.forEach(col => {
      col.getObject("status") should be("PROCESSING")
    })

  }

  "VideoStreamService" should "Process job request" in {

  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }

  def readFromCassandra(event: String): util.List[Row] = {
    val event1 = parse(event).values.asInstanceOf[Map[String, AnyRef]]
    val contentId = event1.get("object").get.asInstanceOf[Map[String, AnyRef]].get("id").get
    val query = s"select * from ${jobConfig.dbKeyspace}.${jobConfig.dbTable} where job_id='${contentId}_1605816926271' ALLOW FILTERING;"
    cassandraUtil.find(query)
  }
}
