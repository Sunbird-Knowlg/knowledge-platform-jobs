package org.sunbird.job.livenodepublisher.spec

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.livenodepublisher.publish.domain.Event
import org.sunbird.job.livenodepublisher.task.{LiveNodePublisherConfig, LiveNodePublisherStreamTask}
import org.sunbird.job.util.HttpUtil
import org.sunbird.spec.BaseTestSpec

import java.text.SimpleDateFormat
import java.util
import java.util.Date

class LiveNodePublisherStreamTaskSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])
  implicit val strTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: LiveNodePublisherConfig = new LiveNodePublisherConfig(config)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }

  "LiveNodePublisherStreamTask" should "generate metrics" ignore {
    val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
    val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    
    import org.apache.flink.connector.kafka.source.KafkaSource
    import org.apache.flink.connector.kafka.sink.KafkaSink
    import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
    import org.sunbird.job.serde.{JobRequestDeserializationSchema, StringSerializationSchema}
    
    val mockSourceV2 = KafkaSource.builder[Event]()
      .setBootstrapServers("localhost:9092")
      .setTopics("dummy")
      .setDeserializer(new JobRequestDeserializationSchema[Event])
      .build()

    val mockSinkV2 = KafkaSink.builder[String]()
      .setBootstrapServers("localhost:9092")
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
        .setTopic("dummy")
        .setValueSerializationSchema(new StringSerializationSchema("dummy"))
        .build())
      .build()

    when(mockKafkaUtil.kafkaJobRequestSourceV2[Event](anyString())(any())).thenReturn(mockSourceV2)
    when(mockKafkaUtil.kafkaStringSinkV2(anyString())).thenReturn(mockSinkV2)
    
    new LiveNodePublisherStreamTask(jobConfig, mockKafkaUtil, mockHttpUtil).process()
  }
}
