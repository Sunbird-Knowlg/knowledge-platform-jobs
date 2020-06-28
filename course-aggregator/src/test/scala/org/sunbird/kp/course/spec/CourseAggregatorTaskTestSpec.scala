package org.sunbird.kp.course.spec

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
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.mockito.Mockito._
import org.sunbird.async.core.cache.RedisConnect
import org.sunbird.async.core.job.FlinkKafkaConnector
import org.sunbird.async.core.util.CassandraUtil
import org.sunbird.async.core.{BaseMetricsReporter, BaseTestSpec}
import org.sunbird.kp.course.fixture.EventFixture
import org.sunbird.kp.course.task.{CourseAggregatorConfig, CourseAggregatorStreamTask}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import scala.collection.JavaConverters._

class CourseAggregatorTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  var redisServer: RedisServer = _
  redisServer = new RedisServer(6373)
  redisServer.start()
  var jedis: Jedis = _
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val gson = new Gson()
  val config: Config = ConfigFactory.load("test.conf")
  val courseAggregatorConfig: CourseAggregatorConfig = new CourseAggregatorConfig(config)


  var cassandraUtil: CassandraUtil = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val redisConnect = new RedisConnect(courseAggregatorConfig)
    jedis = redisConnect.getConnection(courseAggregatorConfig.leafNodesStore)
    setupRedisTestData(jedis)
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(courseAggregatorConfig.dbHost, courseAggregatorConfig.dbPort)
    val session = cassandraUtil.session


    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    // Clear the metrics
    testCassandraUtil(cassandraUtil)
    BaseMetricsReporter.gaugeMetrics.clear()

    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
      redisServer.stop()
    } catch {
      case ex: Exception => {

      }
    }
    flinkCluster.after()
  }


  "Aggregator " should "Compute and update to cassandra database" in {
    when(mockKafkaUtil.kafkaMapSource(courseAggregatorConfig.kafkaInputTopic)).thenReturn(new CourseAggregatorMapSource)
    when(mockKafkaUtil.kafkaMapSink(courseAggregatorConfig.kafkaFailedTopic)).thenReturn(new FailedEventsSink)
    new CourseAggregatorStreamTask(courseAggregatorConfig, mockKafkaUtil).process()
    //val failedEvent = gson.fromJson(gson.toJson(FailedEventsSink.values.get(0)), new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala
    //assert(failedEvent.get("map").get.asInstanceOf[LinkedTreeMap[String, AnyRef]].containsKey("metadata"))
     BaseMetricsReporter.gaugeMetrics(s"${courseAggregatorConfig.jobName}.${courseAggregatorConfig.totalEventsCount}").getValue() should be(1)
    // val test_row1 = cassandraUtil.findOne("select total_score,total_max_score from sunbird_courses.assessment_aggregator where course_id='do_2128410273679114241112'")
    // assert(test_row1.getDouble("total_score") == 2.0)
    // assert(test_row1.getDouble("total_max_score") == 2.0)

    // val test_row2 = cassandraUtil.findOne("select total_score,total_max_score from sunbird_courses.assessment_aggregator where course_id='do_2128415652377067521125'")
    // assert(test_row2.getDouble("total_score") == 3.0)
    // assert(test_row2.getDouble("total_max_score") == 4.0)
  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
    val response = cassandraUtil.find(s"SELECT * FROM ${courseAggregatorConfig.dbKeyspace}.${courseAggregatorConfig.dbTable};")
    println("cassandra resposne" + response)
    response should not be (null)
  }

  def setupRedisTestData(jedis: Jedis) {
    // Insert user test data
    jedis.flushDB()
    jedis.lpush("do_1127212344324751361295:leafnodes", "do_11260735471149056012299")
    jedis.lpush("do_1127212344324751361295:leafnodes", "do_11260735471149056012300")
    jedis.lpush("do_1127212344324751361295:leafnodes", "do_11260735471149056012301")
    jedis.close()
  }
}


class CourseAggregatorMapSource extends SourceFunction[util.Map[String, AnyRef]] {

  override def run(ctx: SourceContext[util.Map[String, AnyRef]]) {
    val gson = new Gson()
    val eventMap1 = gson.fromJson(EventFixture.EVENT_WITH_MESSAGE_ID, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala ++ Map("partition" -> 0.asInstanceOf[AnyRef])
    val eventMap2 = gson.fromJson(EventFixture.EVENT_WITH_MESSAGE_ID, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala ++ Map("partition" -> 2.asInstanceOf[AnyRef])
    val eventMap3 = gson.fromJson(EventFixture.EVENT_WITH_MESSAGE_ID, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala ++ Map("partition" -> 1.asInstanceOf[AnyRef])
    val eventMap4 = gson.fromJson(EventFixture.EVENT_WITH_MESSAGE_ID, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala ++ Map("partition" -> 1.asInstanceOf[AnyRef])
    ctx.collect(eventMap1.asJava)
    ctx.collect(eventMap2.asJava)
    ctx.collect(eventMap3.asJava)
    ctx.collect(eventMap4.asJava)
  }

  override def cancel() = {}

}


class FailedEventsSink extends SinkFunction[util.Map[String, AnyRef]] {

  override def invoke(value: util.Map[String, AnyRef]): Unit = {
    synchronized {
      FailedEventsSink.values.add(value)
    }
  }
}

object FailedEventsSink {
  val values: util.List[util.Map[String, AnyRef]] = new util.ArrayList()
}