package org.sunbird.job.spec

import java.util
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.sunbird.job.util.{HttpUtil, JSONUtil}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.domain.Event
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.task.{AutoCreatorV2Config, AutoCreatorV2StreamTask}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

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

  override protected def beforeAll(): Unit = {
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    flinkCluster.after()
    super.afterAll()
  }

  "event" should " process the input map and return metadata " in {
    val event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2), 0, 1)
    event.isValid should be(true)
    event.action should be("auto-create")
    event.mimeType should be("application/vnd.ekstep.html-archive")
    event.objectId should be("do_113244425048121344131")
    event.objectType should be("QuestionSet")
    event.repository should be("https://dock.sunbirded.org/api/questionset/v1/read/do_113244425048121344131")
    event.downloadUrl should be("https://dockstorage.blob.core.windows.net/sunbird-content-dock/questionset/do_113244425048121344131/added1_1616751462043_do_113244425048121344131_1_SPINE.ecar")
    event.pkgVersion should be(1.0)
    event.eData.size should be(8)
    event.metadata.size should be(63)
  }

   ignore should "generate event" in {
    when(mockKafkaUtil.kafkaMapSource(jobConfig.kafkaInputTopic)).thenReturn(new AutoCreatorV2MapSource)
    new AutoCreatorV2StreamTask(jobConfig, mockKafkaUtil, mockHttpUtil).process()
  }
}

class AutoCreatorV2MapSource extends SourceFunction[util.Map[String, AnyRef]] {

  override def run(ctx: SourceContext[util.Map[String, AnyRef]]) {
  }

  override def cancel(): Unit = {}
}