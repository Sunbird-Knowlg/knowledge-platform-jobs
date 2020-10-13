package org.sunbird.job.spec

import java.io.File
import java.util

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.ArgumentMatchers.{any, endsWith}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.incredible.processor.store.AzureStore
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.task.{CertificateGeneratorConfig, CertificateGeneratorStreamTask}
import org.sunbird.job.util.{HTTPResponse, HttpUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

import scala.collection.JavaConverters._

class CertificateGeneratorFunctionTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val gson = new Gson()
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: CertificateGeneratorConfig = new CertificateGeneratorConfig(config)
  val mockHttpUtil = mock[HttpUtil]
  val azureStore = mock[AzureStore]


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Clear the metrics
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
    when(mockHttpUtil.post(endsWith("/v3/search"), any[String])).thenReturn(HTTPResponse(200, """{"id":"api.search-service.search","ver":"3.0","ts":"2020-08-31T22:09:07ZZ","params":{"resmsgid":"bc9a8ac0-f67d-47d5-b093-2077191bf93b","msgid":null,"err":null,"status":"successful","errmsg":null},"responseCode":"OK","result":{"count":5,"content":[{"identifier":"do_11301367667942195211854","origin":"do_11300581751853056018","channel":"b00bc992ef25f1a9a8d63291e20efc8d","originData":"{\"name\":\"Origin Content\",\"copyType\":\"deep\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"]}","mimeType":"application/vnd.ekstep.content-collection","contentType":"TextBook","objectType":"Content","status":"Draft","versionKey":"1588583579763"},{"identifier":"do_113005885057662976128","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632481597"},{"identifier":"do_113005885161611264130","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632475439"},{"identifier":"do_113005882957578240124","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587632233649"},{"identifier":"do_113005820474007552111","origin":"do_11300581751853056018","channel":"sunbird","originData":"{\"name\":\"Origin Content\",\"copyType\":\"shallow\",\"license\":\"CC BY 4.0\",\"organisation\":[\"Sunbird\"],\"pkgVersion\":2.0}","mimeType":"application/vnd.ekstep.content-collection","lastPublishedBy":"Ekstep","contentType":"TextBook","objectType":"Content","status":"Live","versionKey":"1587624624051"}]}}"""))
    when(azureStore.save(any[File],any[String])).thenReturn("http://basepath/id.json")
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }


  "CertificateGenerator " should "generate certificate and add to the registry" in {
    when(mockKafkaUtil.kafkaMapSource(jobConfig.kafkaInputTopic)).thenReturn(new CertificateGeneratorMapSource)
    new CertificateGeneratorStreamTask(jobConfig, mockKafkaUtil).process()
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(0)
  }


}

class CertificateGeneratorMapSource extends SourceFunction[java.util.Map[String, AnyRef]] {
  override def run(ctx: SourceContext[util.Map[String, AnyRef]]): Unit = {
    val gson = new Gson()
    val eventMap1 = gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala
    ctx.collect(eventMap1.asJava)
  }

  override def cancel() = {}
}