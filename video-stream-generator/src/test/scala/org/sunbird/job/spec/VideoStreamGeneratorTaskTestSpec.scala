package org.sunbird.job.spec

import java.util

import com.datastax.driver.core.Row
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
import org.json4s.jackson.JsonMethods.parse
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.service.IMediaService
import org.sunbird.job.task.{VideoStreamGeneratorConfig, VideoStreamGeneratorStreamTask}
import org.sunbird.job.util.CassandraUtil
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

import scala.collection.JavaConverters._

class VideoStreamGeneratorTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val gson = new Gson()
  val mediaService = mock[IMediaService](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: VideoStreamGeneratorConfig = new VideoStreamGeneratorConfig(config)
  val server = new MockAzureWebServer(jobConfig)
  var cassandraUtil: CassandraUtil = _

  override protected def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    testCassandraUtil(cassandraUtil)
    flinkCluster.before()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => {
      }
    }
    flinkCluster.after()
    super.afterAll()
  }

  override protected def afterEach():Unit = {
    server.close()
    super.afterEach()
  }

  "VideoStreamGenerator" should " submit a job" in {
    server.submitRestUtilData
    when(mockKafkaUtil.kafkaMapSource(jobConfig.kafkaInputTopic)).thenReturn(new VideoStreamGeneratorMapSource)

    new VideoStreamGeneratorStreamTask(jobConfig, mockKafkaUtil).process()
    val event1Progress = readFromCassandra(EventFixture.EVENT_1)
    event1Progress.size() should be(1)
    event1Progress.forEach(col => {
      col.getObject("status") should be("PROCESSING")
    })

  }

  "VideoStreamGenerator" should "process the submitted jobs" in {
    server.processRestUtilData
    when(mockKafkaUtil.kafkaMapSource(jobConfig.kafkaInputTopic)).thenReturn(new VideoStreamGeneratorMapSource)
//    when(mockKafkaUtil.kafkaMapSource(jobConfig.kafkaInputTopic)).thenReturn(new VideoStreamGeneratorEmptyMapSource)
    val dataLoader = new CQLDataLoader(cassandraUtil.session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/job_request.cql").getPath, true, true));
    testCassandraUtil(cassandraUtil)

    new VideoStreamGeneratorStreamTask(jobConfig, mockKafkaUtil).process()
  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }

  def readFromCassandra(event: String): util.List[Row] = {
    val event1 = parse(event).values.asInstanceOf[Map[String, AnyRef]]
    val contentId = event1.get("object").get.asInstanceOf[Map[String, AnyRef]].get("id").get
    val query = s"select * from ${jobConfig.dbKeyspace}.${jobConfig.dbTable} where job_id='${contentId}_1605816926271' ALLOW FILTERING;"
    cassandraUtil.find(query)
  }

}

class VideoStreamGeneratorMapSource extends SourceFunction[util.Map[String, AnyRef]] {

  override def run(ctx: SourceContext[util.Map[String, AnyRef]]) {
    ctx.collect(jsonToMap(EventFixture.EVENT_1))
//    val eventMap1 = parse(EventFixture.EVENT_1).values.asInstanceOf[Map[String, AnyRef]] ++ Map("partition" -> 0.asInstanceOf[AnyRef])
//    ctx.collect(eventMap1)
  }

  def jsonToMap(json: String): util.Map[String, AnyRef] = {
    val gson = new Gson()
    gson.fromJson(json, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
  }

  override def cancel() = {}

}

class VideoStreamGeneratorEmptyMapSource extends SourceFunction[util.Map[String, AnyRef]] {

  override def run(ctx: SourceContext[util.Map[String, AnyRef]]) {
    ctx.collect(jsonToMap(EventFixture.EVENT_2))
//    val eventMap1 = parse("""{}""").values.asInstanceOf[Map[String, AnyRef]] ++ Map("partition" -> 0.asInstanceOf[AnyRef])
//    val eventMap1 = parse(EventFixture.EVENT_2).values.asInstanceOf[Map[String, AnyRef]] ++ Map("partition" -> 0.asInstanceOf[AnyRef])
//    ctx.collect(eventMap1.asJava)
  }

  override def cancel() = {}

  def jsonToMap(json: String): util.Map[String, AnyRef] = {
    val gson = new Gson()
    gson.fromJson(json, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
  }

}



