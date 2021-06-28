package org.sunbird.job.autocreatorv2.spec

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
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.autocreatorv2.domain.Event
import org.sunbird.job.autocreatorv2.fixture.EventFixture
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.task.{AutoCreatorV2Config, AutoCreatorV2StreamTask}
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

import java.util
import scala.collection.JavaConverters._


class AutoCreatorV2TaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: AutoCreatorV2Config = new AutoCreatorV2Config(config)
  var currentMilliSecond = 1605816926271L
  val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  var cassandraUtil: CassandraUtil = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.lmsDbHost, jobConfig.lmsDbPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))

    flinkCluster.before()
    BaseMetricsReporter.gaugeMetrics.clear()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => {
      }
    }
  }

  ignore should " process the input map and return metadata " in {
    val event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2), 0, 1)
    event.isValid should be(true)
    event.action should be("auto-create")
    event.mimeType should be("application/vnd.ekstep.html-archive")
    event.objectId should be("do_113244425048121344131")
    event.objectType should be("QuestionSet")
    event.repository should be (Option("https://dock.sunbirded.org/api/questionset/v1/read/do_113244425048121344131"))
    event.downloadUrl should be("https://dockstorage.blob.core.windows.net/sunbird-content-dock/questionset/do_113244425048121344131/added1_1616751462043_do_113244425048121344131_1_SPINE.ecar")
    event.pkgVersion should be(1.0)
    event.eData.size should be(8)
    event.metadata.size should be(63)
  }

  ignore should "generate event" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new AutoCreatorV2EventSource)
    when(mockHttpUtil.get("https://dock.sunbirded.org/api/questionset/v1/read/do_113244425048121344131")).thenReturn(HTTPResponse(200, """{"id":"api.questionset.read","ver":"3.0","ts":"2021-05-31T12:51:39ZZ","params":{"resmsgid":"b4edb85d-45e2-4113-96db-e30961204bd1","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"questionset":{"unitIdentifiers":["do_1132443997036380161925"],"organisationId":"13495698-a117-460b-920c-41007923c764","previewUrl":"https://dockstorage.blob.core.windows.net/sunbird-content-dock/questionset/do_113244425048121344131/do_113244425048121344131_html_1616751462170.html","keywords":["added1"],"subject":["Mathematics"],"channel":"01309282781705830427","downloadUrl":"https://dockstorage.blob.core.windows.net/sunbird-content-dock/questionset/do_113244425048121344131/added1_1616751462043_do_113244425048121344131_1_SPINE.ecar","language":["English"],"mimeType":"application/vnd.sunbird.questionset","showHints":"No","variants":{"spine":"https://dockstorage.blob.core.windows.net/sunbird-content-dock/questionset/do_113244425048121344131/added1_1616751462043_do_113244425048121344131_1_SPINE.ecar","online":"https://dockstorage.blob.core.windows.net/sunbird-content-dock/questionset/do_113244425048121344131/added1_1616751462123_do_113244425048121344131_1_ONLINE.ecar"},"objectType":"QuestionSet","gradeLevel":["Class 1"],"primaryCategory":"Practice Question Set","contentEncoding":"gzip","showSolutions":"No","identifier":"do_113244425048121344131","audience":["Teacher"],"visibility":"Default","showTimer":"Yes","author":"vaishali","consumerId":"fa13b438-8a3d-41b1-8278-33b0c50210e4","childNodes":["do_113244425563799552132"],"languageCode":["en"],"version":1,"license":"CC BY 4.0","prevState":"Draft","lastPublishedOn":"2021-03-26T09:37:42.028+0000","name":"added1","status":"Live","code":"6c50e01d-e19a-ce7b-04ba-c050317dafd1","allowSkip":"Yes","containsUserData":"No","description":"added1","medium":["Hindi"],"createdOn":"2021-03-26T09:31:44.508+0000","pdfUrl":"https://dockstorage.blob.core.windows.net/sunbird-content-dock/questionset/do_113244425048121344131/do_113244425048121344131_pdf_1616751462170.pdf","contentDisposition":"inline","additionalCategories":["Lesson Plan"],"lastUpdatedOn":"2021-03-26T09:37:42.575+0000","collectionId":"do_1132443997030891521922","allowAnonymousAccess":"Yes","lastStatusChangedOn":"2021-03-26T09:37:42.575+0000","creator":"lily21","requiresSubmit":"No","se_FWIds":["ekstep_ncert_k-12"],"setType":"materialised","pkgVersion":1,"versionKey":"1616751174274","showFeedback":"No","framework":"ekstep_ncert_k-12","depth":0,"createdBy":"d31960d0-613f-4f5d-803d-9354a7bc056d","compatibilityLevel":5,"navigationMode":"non-linear","timeLimits":{"maxTime":"3600"},"shuffle":true,"board":"CBSE","programId":"cb59d300-8e0e-11eb-8137-bd637a1d2ab0"}}}"""))
    new AutoCreatorV2StreamTask(jobConfig, mockKafkaUtil, mockHttpUtil).process()
  }
}

class AutoCreatorV2EventSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]): Unit = {
    val gson = new Gson()
    // Event for Batch Creation and ShallowCopy
    val eventMap1 = gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, Any]().getClass).asInstanceOf[util.Map[String, Any]].asScala ++ Map("partition" -> 0.asInstanceOf[Any])
    ctx.collect(new Event(eventMap1.asJava,0, 10))
  }

  override def cancel(): Unit = {}
}