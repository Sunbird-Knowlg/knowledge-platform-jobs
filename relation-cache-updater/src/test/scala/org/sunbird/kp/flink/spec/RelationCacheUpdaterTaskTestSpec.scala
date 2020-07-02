package org.sunbird.kp.flink.spec

import java.util

import com.datastax.driver.core.Row
import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.async.core.job.FlinkKafkaConnector
import org.sunbird.async.core.{BaseMetricsReporter, BaseTestSpec}
import org.sunbird.kp.flink.fixture.EventFixture
import org.sunbird.kp.flink.task.{RelationCacheUpdaterConfig, RelationCacheUpdaterStreamTask}

import scala.collection.JavaConverters._
import scala.collection.mutable

class RelationCacheUpdaterTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val gson = new Gson()
  val config: Config = ConfigFactory.load("test.conf")
  val cacheUpdaterConfig: RelationCacheUpdaterConfig = new RelationCacheUpdaterConfig(config)


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }


  "Aggregator " should "Compute and update to cassandra database" in {
    when(mockKafkaUtil.kafkaMapSource(cacheUpdaterConfig.kafkaInputTopic)).thenReturn(new RelationCacheUpdaterMapSource)
    new RelationCacheUpdaterStreamTask(cacheUpdaterConfig, mockKafkaUtil).process()
    BaseMetricsReporter.gaugeMetrics(s"${cacheUpdaterConfig.jobName}.${cacheUpdaterConfig.totalEventsCount}").getValue() should be(3)
    BaseMetricsReporter.gaugeMetrics(s"${cacheUpdaterConfig.jobName}.${cacheUpdaterConfig.successEventCount}").getValue() should be(3)
  }

class RelationCacheUpdaterMapSource extends SourceFunction[util.Map[String, AnyRef]] {

  override def run(ctx: SourceContext[util.Map[String, AnyRef]]) {
    val gson = new Gson()
    val eventMap1 = gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala
    val eventMap2 = gson.fromJson(EventFixture.EVENT_2, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala
    val eventMap3 = gson.fromJson(EventFixture.EVENT_3, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala
    ctx.collect(eventMap1.asJava)
    ctx.collect(eventMap2.asJava)
    ctx.collect(eventMap3.asJava)
  }

  override def cancel() = {}

}


class auditEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      auditEventSink.values.add(value)
    }
  }
}

object auditEventSink {
  val values: util.List[String] = new util.ArrayList()
}

class SuccessEvent extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      SuccessEventSink.values.add(value)
    }
  }
}

object SuccessEventSink {
  val values: util.List[String] = new util.ArrayList()
}
}