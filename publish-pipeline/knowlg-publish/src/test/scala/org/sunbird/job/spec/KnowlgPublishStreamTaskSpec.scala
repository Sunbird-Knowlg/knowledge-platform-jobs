package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flink.configuration.Configuration
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito
import org.mockito.Mockito.{when, doAnswer}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.knowlg.task.{KnowlgPublishConfig, KnowlgPublishStreamTask}
import org.sunbird.job.util.{CassandraUtil, CloudStorageUtil, HttpUtil, JanusGraphUtil}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.util
import java.io.File

class HttpUtilStub extends HttpUtil with Serializable {
  override def getSize(url: String, headers: Map[String, String]): Int = 100
  override def downloadFile(url: String, downloadLocation: String): File = {
    val file = new File(downloadLocation)
    if (!file.exists()) {
      file.getParentFile.mkdirs()
      file.createNewFile()
    }
    file
  }
}

class KnowlgPublishStreamTaskSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(new Configuration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: KnowlgPublishConfig = new KnowlgPublishConfig(config)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
  }

  "KnowlgPublishStreamTask" should "generate metrics" ignore {
    val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
    val stubHttpUtil = new HttpUtilStub
    
    import org.apache.flink.connector.kafka.source.KafkaSource
    import org.apache.flink.connector.kafka.sink.KafkaSink
    import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
    import org.sunbird.job.serde.StringSerializationSchema
    import org.sunbird.job.knowlg.publish.domain.Event
    import org.sunbird.job.serde.JobRequestDeserializationSchema

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
    
    new KnowlgPublishStreamTask(jobConfig, mockKafkaUtil, stubHttpUtil).process()
  }
}
