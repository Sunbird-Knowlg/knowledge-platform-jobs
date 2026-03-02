package org.sunbird.job.spec.service

import java.util
import com.datastax.driver.core.Row
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTimeUtils
import org.mockito.ArgumentMatchers.{any, anyString, contains}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.videostream.service.VideoStreamService
import org.sunbird.job.videostream.task.VideoStreamGeneratorConfig
import org.sunbird.spec.BaseTestSpec
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil}
import org.sunbird.job.videostream.domain.Event
import org.sunbird.job.Metrics

class VideoStreamServiceTestSpec extends BaseTestSpec {
  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: VideoStreamGeneratorConfig = new VideoStreamGeneratorConfig(config)
  val mockHttpUtil:HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  val mockMetrics = mock[Metrics](Mockito.withSettings().serializable())
  val mockCassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())

  val accessTokenResp = """{"token_type":"Bearer","expires_in":"3599","ext_expires_in":"3599","expires_on":"1605789466","not_before":"1605785566","resource":"https://management.core.windows.net/","access_token":"testToken"}"""
  val assetJson = """{"name":"asset-do_3126597193576939521910_1605816926271","id":"/subscriptions/aaaaaaaa-6899-4ef6-aaaa-5a185b3b7254/resourceGroups/sunbird-devnew-env/providers/Microsoft.Media/mediaservices/sunbirddevmedia/assets/asset-do_3126597193576939521910_1605816926271","type":"Microsoft.Media/mediaservices/assets","properties":{"assetId":"aaaaaaa-13bb-45c7-aaaa-32ac2e97cf12","created":"2020-11-19T20:16:54.463Z","lastModified":"2020-11-19T20:20:33.613Z","alternateId":"asset-do_3126597193576939521910_1605816926271","description":"Output Asset for do_3126597193576939521910_1605816926271","container":"asset-aaaaaaa-13bb-45c7-b186-32ac2e97cf12","storageAccountName":"sunbirddevmedia","storageEncryptionFormat":"None"}}"""
  val submitJobJson = """{"name":"do_3126597193576939521910_1605816926271","id":"/subscriptions/aaaaaaaa-6899-4ef6-8a14-5a185b3b7254/resourceGroups/sunbird-devnew-env/providers/Microsoft.Media/mediaservices/sunbirddevmedia/transforms/media_transform_default/jobs/do_3126597193576939521910_1605816926271","type":"Microsoft.Media/mediaservices/transforms/jobs","properties":{"created":"2020-11-19T20:26:49.7953248Z","state":"Scheduled","input":{"@odata.type":"#Microsoft.Media.JobInputHttp","files":["test.mp4"],"baseUri":"https://sunbirded.com/"},"lastModified":"2020-11-19T20:26:49.7953248Z","outputs":[{"@odata.type":"#Microsoft.Media.JobOutputAsset","state":"Queued","progress":0,"label":"BuiltInStandardEncoderPreset_0","assetName":"asset-do_3126597193576939521910_1605816926271"}],"priority":"Normal","correlationData":{}}}"""
  val getJobJson = """{"name":"do_3126597193576939521910_1605816926271","job":{"status":"Finished"},"properties":{"created":"2020-11-19T20:26:49.7953248Z","state":"Scheduled","input":{"@odata.type":"#Microsoft.Media.JobInputHttp","files":["test.mp4"],"baseUri":"https://sunbirded.com/"},"lastModified":"2020-11-19T20:26:49.7953248Z","outputs":[{"@odata.type":"#Microsoft.Media.JobOutputAsset","state":"FINISHED","progress":0,"label":"BuiltInStandardEncoderPreset_0","assetName":"asset-do_3126597193576939521910_1605816926271"}],"priority":"Normal","correlationData":{}}}"""
  val getStreamUrlJson = """{"streamingPaths":[{"streamingProtocol":"Hls","encryptionScheme":"NoEncryption","paths":["/4ddff5cd-6479-4572-bc95-ebad508b65ce/ariel-view-of-earth.ism/manifest(format=m3u8-aapl)","/4ddff5cd-6479-4572-bc95-ebad508b65ce/ariel-view-of-earth.ism/manifest(format=m3u8-cmaf)"]},{"streamingProtocol":"Dash","encryptionScheme":"NoEncryption","paths":["/4ddff5cd-6479-4572-bc95-ebad508b65ce/ariel-view-of-earth.ism/manifest(format=mpd-time-csf)","/4ddff5cd-6479-4572-bc95-ebad508b65ce/ariel-view-of-earth.ism/manifest(format=mpd-time-cmaf)"]},{"streamingProtocol":"SmoothStreaming","encryptionScheme":"NoEncryption","paths":["/4ddff5cd-6479-4572-bc95-ebad508b65ce/ariel-view-of-earth.ism/manifest"]}],"downloadPaths":[]}"""
  val getStreamLocatorJson = """{"properties":{"streamingLocatorId":"adcacdd-13bb-45c7-aaaa-32ac2e97cf12"}}"""

  override protected def beforeAll(): Unit = {
    DateTimeUtils.setCurrentMillisFixed(1605816926271L);
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    DateTimeUtils.setCurrentMillisSystem();
    super.afterAll()
  }

  "VideoStreamService" should "submit job request" in {
    when(mockHttpUtil.put(contains(jobConfig.getConfig("azure_mediakind.project_name")+"/assets/asset-"), anyString(), any())).thenReturn(HTTPResponse(200, assetJson))
    when(mockHttpUtil.put(contains("transforms/media_transform_default/jobs"), anyString(), any())).thenReturn(HTTPResponse(200, submitJobJson))
    when(mockHttpUtil.get(contains("transforms/media_transform_default/jobs"), any())).thenReturn(HTTPResponse(200, getJobJson))
    when(mockHttpUtil.post(contains("/streamingLocators/sl-do_3126597193576939521910_1605816926271/listPaths"), any(), any())).thenReturn(HTTPResponse(200, getStreamUrlJson))
    when(mockHttpUtil.put(contains("/streamingLocators/sl-do_3126597193576939521910_1605816926271"), any(), any())).thenReturn(HTTPResponse(400, getJobJson))
    when(mockHttpUtil.get(contains("/streamingLocators/sl-do_3126597193576939521910_1605816926271"), any())).thenReturn(HTTPResponse(200, getStreamLocatorJson))
    when(mockHttpUtil.patch(contains(jobConfig.contentV4Update), any(), any())).thenReturn(HTTPResponse(200, getJobJson))
    doNothing().when(mockMetrics).incCounter(any())

    val mockRow = mock[Row](Mockito.withSettings().serializable())
    when(mockRow.getString("client_key")).thenReturn("SYSTEM_LP", Seq.empty: _*)
    when(mockRow.getString("request_id")).thenReturn("req1", Seq.empty: _*)
    when(mockRow.getString("job_id")).thenReturn("do_3126597193576939521910_1605816926271", Seq.empty: _*)
    when(mockRow.getString("status")).thenReturn("PROCESSING", Seq.empty: _*)
    when(mockRow.getString("request_data")).thenReturn(EventFixture.EVENT_1, Seq.empty: _*)
    when(mockRow.getInt("iteration")).thenReturn(0.asInstanceOf[Integer], Seq.empty: _*)
    when(mockRow.getString("stage")).thenReturn("STREAMING_JOB_SUBMISSION", Seq.empty: _*)
    when(mockRow.getString("stage_status")).thenReturn("PROCESSING", Seq.empty: _*)
    when(mockRow.getString("job_name")).thenReturn("VIDEO_STREAMING", Seq.empty: _*)
    when(mockRow.getObject("status")).thenReturn("FINISHED", Seq.empty: _*)

    when(mockCassandraUtil.find(anyString())).thenReturn(java.util.Arrays.asList(mockRow), Seq.empty: _*)
    when(mockCassandraUtil.upsert(anyString())).thenReturn(true.asInstanceOf[java.lang.Boolean], Seq.empty: _*)
    val mockSession = mock[com.datastax.driver.core.Session]
    val mockResultSet = mock[com.datastax.driver.core.ResultSet]
    when(mockSession.execute(any[com.datastax.driver.core.Statement])).thenReturn(mockResultSet, Seq.empty: _*)
    when(mockResultSet.wasApplied()).thenReturn(true.asInstanceOf[java.lang.Boolean], Seq.empty: _*)
    when(mockCassandraUtil.session).thenReturn(mockSession, Seq.empty: _*)

    val eventMap1 = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1),0, 12)

    val videoStreamService = new VideoStreamService()(jobConfig, mockHttpUtil) {
        override lazy val cassandraUtil = mockCassandraUtil
    }
    videoStreamService.submitJobRequest(eventMap1.eData)
    videoStreamService.processJobRequest(mockMetrics)

    val event1Progress = videoStreamService.cassandraUtil.find("dummy query")
    event1Progress.size() should be(1)

    event1Progress.forEach(col => {
      col.getObject("status") should be("FINISHED")
    })

  }
}
