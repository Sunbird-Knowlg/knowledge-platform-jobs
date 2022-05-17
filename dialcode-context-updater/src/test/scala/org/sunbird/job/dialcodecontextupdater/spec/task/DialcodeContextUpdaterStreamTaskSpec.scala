package org.sunbird.job.dialcodecontextupdater.spec.task

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
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.job.Metrics
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.dialcodecontextupdater.domain.Event
import org.sunbird.job.dialcodecontextupdater.fixture.EventFixture
import org.sunbird.job.dialcodecontextupdater.functions.DialcodeContextUpdaterFunction
import org.sunbird.job.dialcodecontextupdater.spec.helper.TestDialcodeContextUpdater
import org.sunbird.job.dialcodecontextupdater.task.{DialcodeContextUpdaterConfig, DialcodeContextUpdaterStreamTask}
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil, Neo4JUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

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

  implicit val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  var cassandraUtil: CassandraUtil = _
  var mockMetrics: Metrics = mock[Metrics](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    BaseMetricsReporter.gaugeMetrics.clear()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
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
    when(mockNeo4JUtil.getNodeProperties(anyString())).thenReturn(new util.HashMap[String, AnyRef])
    initialize
    new DialcodeContextUpdaterStreamTask(jobConfig, mockKafkaUtil, mockHttpUtil).process()
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.dbHitEventCount}").getValue() should be(1)
  }


  val contextResponse = """{"@context": {"schema": "http://schema.org/","identifier": {"@id": "schema:name#identifier","@type": "schema:name"},"channel": {"@id": "schema:name#channel","@type": "schema:name"},"publisher": {"@id": "schema:name#publisher","@type": "schema:name"},"batchCode": {"@id": "schema:name#batchCode","@type": "schema:name"},"status": {"@id": "schema:name#status","@type": "schema:name"},"generatedOn": {"@id": "schema:name#generatedOn","@type": "schema:name"},"publishedOn": {"@id": "schema:name#publishedOn","@type": "schema:name"},"metadata": "@nest","context": {"@id": "http://schema.org/context","@nest": "metadata","cData": {"identifier": "schema:identifier","name": "schema:name","framework": "schema:framework","board": "schema:board","medium": "schema:medium","subject": "schema:subject","gradeLevel": "schema:gradeLevel"}},"linkedTo": { "@id": "http://schema.org/linkedTo","@nest": "metadata","@context": {"identifier": "http://schema.org/identifier","primaryCategory": "schema:primaryCategory","children": {  "@id": "http://schema.org/linkedToChildren",  "@context": {    "identifier": "http://schema.org/identifier",    "primaryCategory": "schema:primaryCategory"  }}}}}}"""

  val searchResponse = """{"id": "api.content.search","ver": "1.0","ts": "2022-05-17T07:23:08.145Z","params": { "resmsgid": "3331f610-d5b2-11ec-aecf-bf611e734ce0", "msgid": "332ee8d0-d5b2-11ec-bde5-a796d2268843", "status": "successful", "err": null, "errmsg": null},"responseCode": "OK","result": { "collections": [ { "identifier": "do_31307361357388185614238", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "English", "Hindi" ], "createdOn": "2020-07-28T01:34:54.470+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/collection/do_31307361357388185614238/artifact/ahhn1cc.thumb.thumb.thumb.jpg", "size": 3419633, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "contentType": "TextBook" }, { "identifier": "do_31310352462397440011697", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "English", "Hindi" ], "createdOn": "2020-09-08T07:49:05.702+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "size": 4652969, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "State (Chandigarh)" }, { "identifier": "do_313263169135534080136703", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi" ], "createdOn": "2021-04-21T21:06:41.119+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31307361357388185614238/artifact/ahhn1cc.thumb.jpg", "size": 4691606, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "State (Himachal Pradesh)" }, { "identifier": "do_313263167296634880136425", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi" ], "createdOn": "2021-04-21T21:02:56.644+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31307361357388185614238/artifact/ahhn1cc.thumb.jpg", "size": 4691616, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "State (Himachal Pradesh)" }, { "identifier": "do_31312343322655948811154", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi" ], "createdOn": "2020-10-06T10:53:15.042+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31307361357388185614238/artifact/ahhn1cc.thumb.jpg", "size": 4691475, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "State (Arunachal Pradesh)" }, { "identifier": "do_31317212017094656011949", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi" ], "createdOn": "2020-12-14T05:47:05.558+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31307361357388185614238/artifact/ahhn1cc.thumb.jpg", "size": 4691674, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "UT (Andaman and Nicobar Islands)" }, { "identifier": "do_31310347499136614411402", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi", "English" ], "createdOn": "2020-09-08T06:08:07.038+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31307361357388185614238/artifact/ahhn1cc.thumb.jpg", "size": 4691550, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "CBSE" }, { "identifier": "do_31310351153087283211530", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi" ], "createdOn": "2020-09-08T07:22:27.424+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "size": 4652868, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "State (Uttarakhand)" }, { "identifier": "do_31310351900026470411729", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi" ], "createdOn": "2020-09-08T07:37:39.214+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "size": 4652932, "primaryCategory": "Digital Textbook", "name": "Delhi_ रिमझिम (by NCERT)", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "State (Delhi)" } ], "count": 1, "content": [ { "identifier": "do_31307361357558579213961", "primaryCategory": "Textbook Unit", "name": "1-झूला", "mimeType": "application/vnd.ekstep.content-collection", "createdOn": "2020-07-28T01:34:54.676+0000", "objectType": "Content" } ], "facets": [ { "values": [ {  "name": "0125196274181898243",  "count": 1 } ], "name": "channel" } ], "collectionsCount": 9}}"""


  "getContextData" should "return context Information" in {
    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(200, contextResponse))

    val contextFields =  new TestDialcodeContextUpdater().getContextJson(mockHttpUtil, jobConfig).keySet.toList
    val cDataFields =  new TestDialcodeContextUpdater().getContextJson(mockHttpUtil, jobConfig)("cData").asInstanceOf[Map[String, AnyRef]].keySet.toList
    println("DialcodeContextUpdaterSpec:: getContextData:: contextFields: " + contextFields)
    println("DialcodeContextUpdaterSpec:: getContextData:: cDataFields: " + cDataFields)

    assert(contextFields.nonEmpty)
  }

  "updateContext" should "should update the dialcode metadata with context information" in {
    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(200, contextResponse))
    when(mockHttpUtil.post(anyString(), anyString(), any())).thenReturn(HTTPResponse(200, searchResponse))

    val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](EventFixture.DIALCODE_EVENT),0,1)

    val dialcodeMetadataRow = new DialcodeContextUpdaterFunction(jobConfig, mockHttpUtil, mockNeo4JUtil, cassandraUtil).updateContext(jobConfig,event, mockHttpUtil, mockNeo4JUtil, cassandraUtil, mockMetrics)
    println("Metadata:: " + dialcodeMetadataRow.getString("metadata"))

    assert(dialcodeMetadataRow.getString("metadata") != null && dialcodeMetadataRow.getString("metadata").nonEmpty)

  }

}


private class DialcodeContextUpdaterEventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {
    ctx.collect(jsonToEvent(EventFixture.DIALCODE_EVENT))
  }

  override def cancel() = {}

  def jsonToEvent(json: String): Event = {
    val gson = new Gson()
    val data = gson.fromJson(json, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]]
    new Event(data, 0, 10)
  }
}

