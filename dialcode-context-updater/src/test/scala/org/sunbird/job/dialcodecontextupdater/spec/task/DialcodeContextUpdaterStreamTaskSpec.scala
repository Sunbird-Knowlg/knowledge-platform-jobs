package org.sunbird.job.dialcodecontextupdater.spec.task

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.ArgumentMatchers.{any, anyString, contains}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.job.Metrics
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.dialcodecontextupdater.domain.Event
import org.sunbird.job.dialcodecontextupdater.fixture.EventFixture
import org.sunbird.job.dialcodecontextupdater.functions.DialcodeContextUpdaterFunction
import org.sunbird.job.dialcodecontextupdater.task.{DialcodeContextUpdaterConfig, DialcodeContextUpdaterStreamTask}
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil, JanusGraphUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}
import com.datastax.driver.core.Row

import java.util

class DialcodeContextUpdaterStreamTaskSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val strTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  implicit val jobConfig: DialcodeContextUpdaterConfig = new DialcodeContextUpdaterConfig(config)

  implicit val mockHttpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  val mockJanusGraphUtil: JanusGraphUtil = mock[JanusGraphUtil](Mockito.withSettings().serializable())
  implicit val cassandraUtil: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())
  var mockMetrics: Metrics = mock[Metrics](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    BaseMetricsReporter.gaugeMetrics.clear()
    val mockRow = mock[Row]
    when(mockRow.getString("hierarchy")).thenReturn("")
    when(cassandraUtil.findOne(anyString())).thenReturn(mockRow)
    when(mockJanusGraphUtil.getNodesName(any())).thenReturn(Map("do_1234" -> "Some Content"))
    flinkCluster.before()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    flinkCluster.after()
    super.afterAll()
  }


  def initialize(): Unit = {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new DialcodeContextUpdaterEventSource)
  }

  ignore should " update the dial context " in {
    when(mockJanusGraphUtil.getNodeProperties(anyString())).thenReturn(new util.HashMap[String, AnyRef])
    initialize()
    new DialcodeContextUpdaterStreamTask(jobConfig, mockKafkaUtil, mockHttpUtil).process()
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.dbHitEventCount}").getValue() should be(1)
  }

  val searchResponse = """{"id": "api.content.search","ver": "1.0","ts": "2022-05-17T07:23:08.145Z","params": { "resmsgid": "3331f610-d5b2-11ec-aecf-bf611e734ce0", "msgid": "332ee8d0-d5b2-11ec-bde5-a796d2268843", "status": "successful", "err": null, "errmsg": null},"responseCode": "OK","result": { "count": 1, "content": [ { "identifier": "do_1234", "primaryCategory": "Digital Textbook" } ], "collections": [ { "identifier": "do_1234", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "English", "Hindi" ], "createdOn": "2020-07-28T01:34:54.470+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/collection/do_31307361357388185614238/artifact/ahhn1cc.thumb.thumb.thumb.jpg", "size": 3419633, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "contentType": "TextBook" } ] } }"""
  val nullEventString = """{"eid":"BE_JOB_REQUEST","ets":1654669707334,"mid":"LP.1654669707334.589dbe02-14b2-4d1f-b4ce-cdda94a1fd73","actor":{"id":"DIAL code context update Job","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"},"channel":"sunbird","env":"dev"},"object":{"ver":"1.0","id":"B8V3F7"},"edata":{"action":"dialcode-context-delete","iteration":1,"dialcode":"B8V3F7","identifier": "do_113555052123987968128"},"identifier": "do_113555052123987968128"}"""
  val dialcodeContextUpdateResponse = """{"id": "api.dialcode.update","ver": "1.0","ts": "2022-06-02T06:56:32.767Z","params": {"resmsgid": "22e298f0-e241-11ec-87a6-19c1a5d877c9","msgid": "22b79150-e241-11ec-a314-3942d93c4e08","status": "successful","err": null,"errmsg": null},"responseCode": "OK","result": {"identifier": "X5I6I4"}}"""
  val dialcodeNullContextReadResponse = """{"id": "api.dialcode.read","ver": "1.0","ts": "2022-06-02T06:37:02.149Z","params": {"resmsgid": "69247750-e23e-11ec-a3e1-bf0e5a4247e4","msgid": "6920cdd0-e23e-11ec-b434-c9d6df1d95b1","status": "successful","err": null,"errmsg": null},"responseCode": "OK","result": {"dialcode": {"identifier": "0117CH01","contextInfo": null,"generatedOn": "2020-03-04T14:15:00.000","batchCode": "0125196274181898243-01","channel": "0125196274181898243","publisher": "0125196274181898243","publishedOn": null,"status": "Draft"}}}"""
  val dialcodeNotNullContextReadResponse = """{"id": "api.dialcode.read","ver": "1.0","ts": "2022-06-02T06:58:30.080Z","params": {"resmsgid": "68cf2400-e241-11ec-87a6-19c1a5d877c9","msgid": "68cdc470-e241-11ec-a314-3942d93c4e08","status": "successful","err": null,"errmsg": null},"responseCode": "OK","result": {"dialcode": {"identifier": "X5I6I4","contextInfo": [{"gradeLevel": ["Class 2"],"@type": "https://staging.sunbirded.org/ns/collection","subject": ["Mathematics"],"medium": ["English"],"@context": "https://sunbirdstagingpublic.blob.core.windows.net/sunbird-dial-staging/schemas/local/collection/context.json","board": "CBSE"}],"generatedOn": "2020-08-26T08:24:48.338+0000","@type": "https://staging.sunbirded.org/ns/DIAL","batchCode": "do_2130943402930012161605","channel": "01272777697873100812","publisher": null,"publishedOn": null,"@id": "https://staging.sunbirded.org/dial/X5I6I4","@context": "https://sunbirdstagingpublic.blob.core.windows.net/sunbird-dial-staging/schemas/local/dialcode/context.json","status": "Draft"}}}"""

  "updateContext" should "should update the dialcode metadata with context information" in {
    when(mockHttpUtil.get(contains(jobConfig.dialcodeContextReadPath), any())).thenReturn(HTTPResponse(200, dialcodeNotNullContextReadResponse))
    when(mockHttpUtil.post(anyString(), anyString(), any())).thenReturn(HTTPResponse(200, searchResponse))
    when(mockHttpUtil.patch(anyString(), anyString(), any())).thenReturn(HTTPResponse(200, dialcodeContextUpdateResponse))

    val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventFixture.DIALCODE_EVENT_WITH_CONTEXT),0,1)

    val dialcodeContextInfo: Map[String, AnyRef] = new DialcodeContextUpdaterFunction(jobConfig, mockHttpUtil).updateContext(jobConfig,event, mockHttpUtil)

    assert(dialcodeContextInfo.contains("contextInfo") && dialcodeContextInfo("contextInfo") != null)
  }

  "updateContext" should "should update the dialcode metadata with null" in {
    when(mockHttpUtil.get(contains(jobConfig.dialcodeContextReadPath), any())).thenReturn(HTTPResponse(200, dialcodeNullContextReadResponse))
    when(mockHttpUtil.post(anyString(), anyString(), any())).thenReturn(HTTPResponse(200, searchResponse))
    when(mockHttpUtil.patch(anyString(), anyString(), any())).thenReturn(HTTPResponse(200, dialcodeContextUpdateResponse))

    val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventFixture.DIALCODE_EVENT_NULL_CONTEXT),0,1)

    val dialcodeContextInfo: Map[String, AnyRef] = new DialcodeContextUpdaterFunction(jobConfig, mockHttpUtil).updateContext(jobConfig,event, mockHttpUtil)

    assert(dialcodeContextInfo.contains("contextInfo") && dialcodeContextInfo("contextInfo") == null)
  }

}


private class DialcodeContextUpdaterEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    ctx.collect(jsonToEvent(EventFixture.DIALCODE_EVENT_WITH_CONTEXT))
  }

  override def cancel(): Unit = {}

  def jsonToEvent(json: String): Event = {
    val gson = new Gson()
    val data = gson.fromJson(json, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]]
    new Event(data, 0, 10)
  }
}
