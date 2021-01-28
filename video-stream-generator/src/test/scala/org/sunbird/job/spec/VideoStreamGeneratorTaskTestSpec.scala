package org.sunbird.job.spec

import java.util

import com.datastax.driver.core.Row
import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.json4s.jackson.JsonMethods.parse
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.{any, anyString, contains}
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.domain.Event
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.service.IMediaService
import org.sunbird.job.task.{VideoStreamGeneratorConfig, VideoStreamGeneratorStreamTask}
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}
import org.joda.time.DateTimeUtils

import scala.collection.JavaConverters._

class VideoStreamGeneratorTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val gson = new Gson()
  val mediaService = mock[IMediaService](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: VideoStreamGeneratorConfig = new VideoStreamGeneratorConfig(config)
//  val server = new MockAzureWebServer(jobConfig)
  var cassandraUtil: CassandraUtil = _
  val httpUtil = new HttpUtil
  implicit val mockHttpUtil:HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  var currentMilliSecond = 1605816926271L

  val accessTokenResp = """{"token_type":"Bearer","expires_in":"3599","ext_expires_in":"3599","expires_on":"1605789466","not_before":"1605785566","resource":"https://management.core.windows.net/","access_token":"testToken"}"""
  val assetJson = """{"name":"asset-do_3126597193576939521910_1605816926271","id":"/subscriptions/aaaaaaaa-6899-4ef6-aaaa-5a185b3b7254/resourceGroups/sunbird-devnew-env/providers/Microsoft.Media/mediaservices/sunbirddevmedia/assets/asset-do_3126597193576939521910_1605816926271","type":"Microsoft.Media/mediaservices/assets","properties":{"assetId":"aaaaaaa-13bb-45c7-aaaa-32ac2e97cf12","created":"2020-11-19T20:16:54.463Z","lastModified":"2020-11-19T20:20:33.613Z","alternateId":"asset-do_3126597193576939521910_1605816926271","description":"Output Asset for do_3126597193576939521910_1605816926271","container":"asset-aaaaaaa-13bb-45c7-b186-32ac2e97cf12","storageAccountName":"sunbirddevmedia","storageEncryptionFormat":"None"}}"""
  val submitJobJson = """{"name":"do_3126597193576939521910_1605816926271","id":"/subscriptions/aaaaaaaa-6899-4ef6-8a14-5a185b3b7254/resourceGroups/sunbird-devnew-env/providers/Microsoft.Media/mediaservices/sunbirddevmedia/transforms/media_transform_default/jobs/do_3126597193576939521910_1605816926271","type":"Microsoft.Media/mediaservices/transforms/jobs","properties":{"created":"2020-11-19T20:26:49.7953248Z","state":"Scheduled","input":{"@odata.type":"#Microsoft.Media.JobInputHttp","files":["test.mp4"],"baseUri":"https://sunbirded.com/"},"lastModified":"2020-11-19T20:26:49.7953248Z","outputs":[{"@odata.type":"#Microsoft.Media.JobOutputAsset","state":"Queued","progress":0,"label":"BuiltInStandardEncoderPreset_0","assetName":"asset-do_3126597193576939521910_1605816926271"}],"priority":"Normal","correlationData":{}}}"""
  val getJobJson = """{"name":"do_3126597193576939521910_1605816926271","job":{"status":"Finished"},"properties":{"created":"2020-11-19T20:26:49.7953248Z","state":"Scheduled","input":{"@odata.type":"#Microsoft.Media.JobInputHttp","files":["test.mp4"],"baseUri":"https://sunbirded.com/"},"lastModified":"2020-11-19T20:26:49.7953248Z","outputs":[{"@odata.type":"#Microsoft.Media.JobOutputAsset","state":"FINISHED","progress":0,"label":"BuiltInStandardEncoderPreset_0","assetName":"asset-do_3126597193576939521910_1605816926271"}],"priority":"Normal","correlationData":{}}}"""
  val getStreamUrlJson = """{"streamingPaths":[{"streamingProtocol":"Hls","encryptionScheme":"NoEncryption","paths":["/4ddff5cd-6479-4572-bc95-ebad508b65ce/ariel-view-of-earth.ism/manifest(format=m3u8-aapl)","/4ddff5cd-6479-4572-bc95-ebad508b65ce/ariel-view-of-earth.ism/manifest(format=m3u8-cmaf)"]},{"streamingProtocol":"Dash","encryptionScheme":"NoEncryption","paths":["/4ddff5cd-6479-4572-bc95-ebad508b65ce/ariel-view-of-earth.ism/manifest(format=mpd-time-csf)","/4ddff5cd-6479-4572-bc95-ebad508b65ce/ariel-view-of-earth.ism/manifest(format=mpd-time-cmaf)"]},{"streamingProtocol":"SmoothStreaming","encryptionScheme":"NoEncryption","paths":["/4ddff5cd-6479-4572-bc95-ebad508b65ce/ariel-view-of-earth.ism/manifest"]}],"downloadPaths":[]}"""
  val getStreamLocatorJson = """{"properties":{"streamingLocatorId":"adcacdd-13bb-45c7-aaaa-32ac2e97cf12"}}"""

  override protected def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    testCassandraUtil(cassandraUtil)
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => {
      }
    }
    flinkCluster.after()
    super.afterAll()
  }

  override protected def afterEach():Unit = {
//    server.close()
    super.afterEach()
  }

  "VideoStreamGenerator" should "submit a job" in {
//    server.submitRestUtilData
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new VideoStreamGeneratorMapSource)

    when(mockHttpUtil.post_map(contains("/oauth2/token"), any[Map[String, AnyRef]](), any[Map[String, String]]())).thenReturn(HTTPResponse(200, accessTokenResp))
    when(mockHttpUtil.put(contains("/providers/Microsoft.Media/mediaServices/"+jobConfig.getSystemConfig("azure.account.name")+"/assets/asset-"), anyString(), any())).thenReturn(HTTPResponse(200, assetJson))
    when(mockHttpUtil.put(contains("transforms/media_transform_default/jobs"), anyString(), any())).thenReturn(HTTPResponse(200, submitJobJson))
    when(mockHttpUtil.get(contains("transforms/media_transform_default/jobs"), any())).thenReturn(HTTPResponse(200, getJobJson))

    when(mockHttpUtil.post(contains("/streamingLocators/sl-do_3126597193576939521910_1605816926271/listPaths?api-version="), any(), any())).thenReturn(HTTPResponse(200, getStreamUrlJson))
    when(mockHttpUtil.put(contains("/streamingLocators/sl-do_3126597193576939521910_1605816926271?api-version="), any(), any())).thenReturn(HTTPResponse(400, getJobJson))
    when(mockHttpUtil.get(contains("/streamingLocators/sl-do_3126597193576939521910_1605816926271?api-version="), any())).thenReturn(HTTPResponse(200, getStreamLocatorJson))
    when(mockHttpUtil.put(contains(jobConfig.contentV3Update), any(), any())).thenReturn(HTTPResponse(200, getJobJson))

    new VideoStreamGeneratorStreamTask(jobConfig, mockKafkaUtil, mockHttpUtil).process()
    val event1Progress = readFromCassandra(EventFixture.EVENT_1)

//    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(2)
//    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(1)
    event1Progress.size() should be(1)

    event1Progress.forEach(col => {
      col.getObject("status") should be("FINISHED")
    })

  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }

  def readFromCassandra(event: String): util.List[Row] = {
    val event1 = parse(event).values.asInstanceOf[Map[String, AnyRef]]
    val contentId = event1.get("object").get.asInstanceOf[Map[String, AnyRef]].get("id").get
    val query = s"select * from ${jobConfig.dbKeyspace}.${jobConfig.dbTable} where job_id='${contentId}_$currentMilliSecond' ALLOW FILTERING;"
    cassandraUtil.find(query)
  }

}

class VideoStreamGeneratorMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {
    val gson = new Gson()
    val event = gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]].asScala ++ Map("partition" -> 0.asInstanceOf[Any])
    val event2 = gson.fromJson(EventFixture.EVENT_2, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]].asScala ++ Map("partition" -> 0.asInstanceOf[Any])
    ctx.collect(new Event(event.asJava))
    ctx.collect(new Event(event2.asJava))

  }

  override def cancel() = {}

}