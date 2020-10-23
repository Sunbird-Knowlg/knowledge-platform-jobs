package org.sunbird.job.spec

import java.io.File
import java.util

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.ArgumentMatchers.{any, endsWith}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.incredible.processor.store.{AzureStore, ICertStore}
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.StorageService
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
  val mockHttpUtil: HttpUtil = mock[HttpUtil]
  var certStore: ICertStore = mock[AzureStore]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Clear the metrics
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
    when(mockHttpUtil.post(endsWith("/certs/v2/registry/add"), any[String])).thenReturn(HTTPResponse(200, """{"id":"api.certs.registry.add","ver":"v2","ts":"1602590393507","params":null,"responseCode":"OK","result":{"id":"c96d60f8-9c76-4a73-9ef0-9e01d0f726c6"}}"""))
    when(certStore.save(any[File], any[String])).thenReturn("http://basepath/uuid.json")
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }


  "CertificateGenerator " should "generate certificate and add to the registry" in {
    CertificateGeneratorStreamTask.httpUtil = mockHttpUtil
    CertificateGeneratorStreamTask.certStore = certStore
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaFailedEventTopic)).thenReturn(new failedEventSink)
    when(mockKafkaUtil.kafkaStringSink(jobConfig.kafkaPostCertificateProcessEventTopic)).thenReturn(new postCertificateProcessEventSink)
    when(mockKafkaUtil.kafkaMapSource(jobConfig.kafkaInputTopic)).thenReturn(new CertificateGeneratorMapSource)
    new CertificateGeneratorStreamTask(jobConfig, mockKafkaUtil).process()
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(2)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(1)
    failedEventSink.values.size() should be(1)
    postCertificateProcessEventSink.values.size() should be (1)
  }


}

class CertificateGeneratorMapSource extends SourceFunction[java.util.Map[String, AnyRef]] {
  override def run(ctx: SourceContext[util.Map[String, AnyRef]]): Unit = {
    val gson = new Gson()
    val eventMap1 = gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala
    val eventMap2 = gson.fromJson(EventFixture.EVENT_2, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala
    ctx.collect(eventMap1.asJava)
    ctx.collect(eventMap2.asJava)
  }

  override def cancel() = {}
}

class failedEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      failedEventSink.values.add(value)
    }
  }
}

object failedEventSink {
  val values: util.List[String] = new util.ArrayList()
}

class postCertificateProcessEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      postCertificateProcessEventSink.values.add(value)
    }
  }
}

object postCertificateProcessEventSink {
  val values: util.List[String] = new util.ArrayList()
}