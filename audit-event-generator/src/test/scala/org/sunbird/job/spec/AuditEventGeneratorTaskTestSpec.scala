package org.sunbird.job.spec

import java.util
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.sunbird.job.util.JSONUtil
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.task.{AuditEventGeneratorConfig, AuditEventGeneratorStreamTask}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

class AuditEventGeneratorTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: AuditEventGeneratorConfig = new AuditEventGeneratorConfig(config)
  var currentMilliSecond = 1605816926271L

  override protected def beforeAll(): Unit = {
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    flinkCluster.after()
    super.afterAll()
  }

  ignore should "generate audit event" in {
    when(mockKafkaUtil.kafkaMapSource(jobConfig.kafkaInputTopic)).thenReturn(new AuditEventGeneratorMapSource)

    new AuditEventGeneratorStreamTask(jobConfig, mockKafkaUtil).process()
  }
}

class AuditEventGeneratorMapSource extends SourceFunction[util.Map[String, AnyRef]] {

  override def run(ctx: SourceContext[util.Map[String, AnyRef]]) {
    ctx.collect(JSONUtil.deserialize[util.Map[String, AnyRef]](EventFixture.EVENT_1))
  }

  override def cancel(): Unit = {}
}