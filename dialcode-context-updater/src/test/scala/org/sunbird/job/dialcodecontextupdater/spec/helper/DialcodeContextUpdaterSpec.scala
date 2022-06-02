package org.sunbird.job.dialcodecontextupdater.spec.helper

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.Metrics
import org.sunbird.job.dialcodecontextupdater.domain.Event
import org.sunbird.job.dialcodecontextupdater.helpers.DialcodeContextUpdater
import org.sunbird.job.dialcodecontextupdater.task.DialcodeContextUpdaterConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.exception.{APIException, ServerException}
import org.sunbird.job.util._
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}


class DialcodeContextUpdaterSpec extends BaseTestSpec {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: DialcodeContextUpdaterConfig = new DialcodeContextUpdaterConfig(config)
  val defCache = new DefinitionCache()
  var mockHttpUtil: HttpUtil = mock[HttpUtil]
  var mockMetrics: Metrics = mock[Metrics](Mockito.withSettings().serializable())
  var cassandraUtil: CassandraUtil = _

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


  val eventString = """{"eid":"BE_JOB_REQUEST","ets":1648720639981,"mid":"LP.1648720639981.d6b1d8c8-7a4a-483a-b83a-b752bede648c","actor":{"id":"DIALcodecontextupdateJob","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":"01269878797503692810","env":"dev"},"object":{"ver":"1.0","id":"0117CH01"},"edata":{"action":"dialcode-context-update","iteration":1,"dialcode":"0117CH01","identifier":"d0_1234","traceId":"2342345345"}}"""
  val searchResponse = """{"id": "api.content.search","ver": "1.0","ts": "2022-05-17T07:23:08.145Z","params": { "resmsgid": "3331f610-d5b2-11ec-aecf-bf611e734ce0", "msgid": "332ee8d0-d5b2-11ec-bde5-a796d2268843", "status": "successful", "err": null, "errmsg": null},"responseCode": "OK","result": { "collections": [ { "identifier": "do_31307361357388185614238", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "English", "Hindi" ], "createdOn": "2020-07-28T01:34:54.470+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/collection/do_31307361357388185614238/artifact/ahhn1cc.thumb.thumb.thumb.jpg", "size": 3419633, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "contentType": "TextBook" }, { "identifier": "do_31310352462397440011697", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "English", "Hindi" ], "createdOn": "2020-09-08T07:49:05.702+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "size": 4652969, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "State (Chandigarh)" }, { "identifier": "do_313263169135534080136703", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi" ], "createdOn": "2021-04-21T21:06:41.119+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31307361357388185614238/artifact/ahhn1cc.thumb.jpg", "size": 4691606, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "State (Himachal Pradesh)" }, { "identifier": "do_313263167296634880136425", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi" ], "createdOn": "2021-04-21T21:02:56.644+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31307361357388185614238/artifact/ahhn1cc.thumb.jpg", "size": 4691616, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "State (Himachal Pradesh)" }, { "identifier": "do_31312343322655948811154", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi" ], "createdOn": "2020-10-06T10:53:15.042+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31307361357388185614238/artifact/ahhn1cc.thumb.jpg", "size": 4691475, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "State (Arunachal Pradesh)" }, { "identifier": "do_31317212017094656011949", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi" ], "createdOn": "2020-12-14T05:47:05.558+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31307361357388185614238/artifact/ahhn1cc.thumb.jpg", "size": 4691674, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "UT (Andaman and Nicobar Islands)" }, { "identifier": "do_31310347499136614411402", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi", "English" ], "createdOn": "2020-09-08T06:08:07.038+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "appIcon": "https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31307361357388185614238/artifact/ahhn1cc.thumb.jpg", "size": 4691550, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "CBSE" }, { "identifier": "do_31310351153087283211530", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi" ], "createdOn": "2020-09-08T07:22:27.424+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "size": 4652868, "primaryCategory": "Digital Textbook", "name": "(NEW) रिमझिम", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "State (Uttarakhand)" }, { "identifier": "do_31310351900026470411729", "subject": [ "Hindi" ], "childNodes": [ "do_31307361357558579213961" ], "origin": "do_31307361357388185614238", "mimeType": "application/vnd.ekstep.content-collection", "medium": [ "Hindi" ], "createdOn": "2020-09-08T07:37:39.214+0000", "objectType": "Collection", "gradeLevel": [ "Class 1" ], "size": 4652932, "primaryCategory": "Digital Textbook", "name": "Delhi_ रिमझिम (by NCERT)", "originData": "{\"name\":\"(NEW) रिमझिम\",\"copyType\":\"shallow\",\"license\":\"CC BY-SA 4.0\",\"organisation\":[\"NCERT\"],\"pkgVersion\":44.0}", "contentType": "TextBook", "board": "State (Delhi)" } ], "count": 1, "content": [ { "identifier": "do_31307361357558579213961", "primaryCategory": "Textbook Unit", "name": "1-झूला", "mimeType": "application/vnd.ekstep.content-collection", "createdOn": "2020-07-28T01:34:54.676+0000", "objectType": "Content" } ], "facets": [ { "values": [ {  "name": "0125196274181898243",  "count": 1 } ], "name": "channel" } ], "collectionsCount": 9}}"""
  val emptySearchResponse = """{ "id": "api.content.search", "ver": "1.0", "ts": "2022-05-18T05:36:30.364Z", "params": {"resmsgid": "783bf4d0-d66c-11ec-a3e1-bf0e5a4247e4","msgid": "783a9540-d66c-11ec-b434-c9d6df1d95b1","status": "successful","err": null,"errmsg": null }, "responseCode": "OK", "result": {"count": 0 }}"""
  val dialcodeNullContextReadResponse = """{"id": "api.dialcode.read","ver": "1.0","ts": "2022-06-02T06:37:02.149Z","params": {"resmsgid": "69247750-e23e-11ec-a3e1-bf0e5a4247e4","msgid": "6920cdd0-e23e-11ec-b434-c9d6df1d95b1","status": "successful","err": null,"errmsg": null},"responseCode": "OK","result": {"dialcode": {"identifier": "0117CH01","contextInfo": null,"generatedOn": "2020-03-04T14:15:00.000","batchCode": "0125196274181898243-01","channel": "0125196274181898243","publisher": "0125196274181898243","publishedOn": null,"status": "Draft"}}}"""



  "getContextJson" should "return context Information" in {
    println("DialcodeContextUpdater:: updateContext:: config.contextMapFilePath: " + jobConfig.contextMapFilePath)
    val contextMap =  new TestDialcodeContextUpdater().getContextMapFields("course_unit", jobConfig.contextMapFilePath)
    val contextFields = new TestDialcodeContextUpdater().fetchFieldsFromMap(contextMap).distinct.filter(rec => rec.forall(_.isLetterOrDigit))
   println("DialcodeContextUpdaterSpec:: getContextData:: contextFields: " + contextFields)

    assert(contextFields.nonEmpty)
  }

//  "getContextJson" should "should throw API exception when there is no context response" in {
//    when(mockHttpUtil.get(anyString(), any())).thenReturn(HTTPResponse(200, ""))
//
//    assertThrows[APIException] {
//     new TestDialcodeContextUpdater().getContextJson(mockHttpUtil, jobConfig).keySet.toList
//    }
//  }

  "updateContext" should "should throw server exception when there is no search response" in {
    when(mockHttpUtil.post(anyString(), anyString(), any())).thenReturn(HTTPResponse(200, emptySearchResponse))

    val event = new Event(JSONUtil.deserialize[java.util.Map[String, Any]](eventString),0,1)

    assertThrows[ServerException] {
      new TestDialcodeContextUpdater().updateContext(jobConfig,event, mockHttpUtil)
    }
  }

}

class TestDialcodeContextUpdater extends DialcodeContextUpdater {}