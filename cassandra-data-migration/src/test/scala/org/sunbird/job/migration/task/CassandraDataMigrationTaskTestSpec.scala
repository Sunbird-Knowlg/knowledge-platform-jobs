package org.sunbird.job.migration.task

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.anyString
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.migration.domain.Event
import org.sunbird.job.task.{CassandraDataMigrationConfig, CassandraDataMigrationStreamTask}
import org.sunbird.job.util.CassandraUtil
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}
import com.datastax.driver.core.Session

import java.util

class CassandraDataMigrationTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: CassandraDataMigrationConfig = new CassandraDataMigrationConfig(config)
  implicit val cassandraUtils: CassandraUtil = mock[CassandraUtil](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    BaseMetricsReporter.gaugeMetrics.clear()
    when(cassandraUtils.session).thenReturn(mock[Session])
    flinkCluster.before()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    flinkCluster.after()
    super.afterAll()
  }

  def initialize(): Unit = {
    import org.apache.flink.connector.kafka.source.KafkaSource
    import org.sunbird.job.serde.JobRequestDeserializationSchema
    import org.mockito.ArgumentMatchers.any
    
    val mockSourceV2 = KafkaSource.builder[Event]()
      .setBootstrapServers("localhost:9092")
      .setTopics("dummy")
      .setDeserializer(new JobRequestDeserializationSchema[Event])
      .build()
      
    when(mockKafkaUtil.kafkaJobRequestSourceV2[Event](anyString())(any())).thenReturn(mockSourceV2)
  }

  "CassandraDataMigrationTask" should "generate event" ignore {
    initialize()
    new CassandraDataMigrationStreamTask(jobConfig, mockKafkaUtil).process()
  }
}
