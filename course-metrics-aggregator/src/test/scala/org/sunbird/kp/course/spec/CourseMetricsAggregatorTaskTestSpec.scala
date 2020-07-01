package org.sunbird.kp.course.spec

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
import org.sunbird.kp.course.task.{CourseMetricsAggregatorConfig, CourseMetricsAggregatorStreamTask}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import scala.collection.JavaConverters._
import scala.collection.mutable

class CourseAggregatorTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  var redisServer: RedisServer = _
  redisServer = new RedisServer(6375)
  redisServer.start()
  var jedis: Jedis = _
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val gson = new Gson()
  val config: Config = ConfigFactory.load("test.conf")
  val courseAggregatorConfig: CourseMetricsAggregatorConfig = new CourseMetricsAggregatorConfig(config)


  var cassandraUtil: CassandraUtil = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val redisConnect = new RedisConnect(courseAggregatorConfig)
    jedis = redisConnect.getConnection(courseAggregatorConfig.nodeStore)
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(courseAggregatorConfig.dbHost, courseAggregatorConfig.dbPort)
    val session = cassandraUtil.session


    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    // Clear the metrics
    testCassandraUtil(cassandraUtil)
    BaseMetricsReporter.gaugeMetrics.clear()
    jedis.flushDB()
    flinkCluster.before()
    updateRedis(jedis, EventFixture.CASE_1.asInstanceOf[Map[String, AnyRef]])
    updateRedis(jedis, EventFixture.CASE_2.asInstanceOf[Map[String, AnyRef]])
    updateRedis(jedis, EventFixture.CASE_3.asInstanceOf[Map[String, AnyRef]])
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
    when(mockKafkaUtil.kafkaStringSink(courseAggregatorConfig.kafkaAuditEventTopic)).thenReturn(new auditEventSink)
    new CourseMetricsAggregatorStreamTask(courseAggregatorConfig, mockKafkaUtil).process()
    BaseMetricsReporter.gaugeMetrics(s"${courseAggregatorConfig.jobName}.${courseAggregatorConfig.totalEventsCount}").getValue() should be(3)
    BaseMetricsReporter.gaugeMetrics(s"${courseAggregatorConfig.jobName}.${courseAggregatorConfig.dbUpdateCount}").getValue() should be(3)
    BaseMetricsReporter.gaugeMetrics(s"${courseAggregatorConfig.jobName}.${courseAggregatorConfig.cacheHitCount}").getValue() should be(18)
    BaseMetricsReporter.gaugeMetrics(s"${courseAggregatorConfig.jobName}.${courseAggregatorConfig.successEventCount}").getValue() should be(3)

    auditEventSink.values.size() should be(15)
    auditEventSink.values.forEach(event =>{
      println("AUDIEBT" + event)
    })

    val event1Progress = readFromCassandra(EventFixture.EVENT_2)
    event1Progress.size() should be(3)
    event1Progress.forEach(col => {
      if (col.getObject("activity_id") == "do_course_unit1") {
        col.getObject("activity_type") should be("course-unit")
        println("aggMap", col.getObject("agg"))
        col.getObject("activity_type") should be("{'progress': 1}")
      }
      if (col.getObject("activity_id") == "do_course_unit2") {
        col.getObject("activity_type") should be("course-unit")
        println("aggMap", col.getObject("agg"))
        col.getObject("activity_type") should be("{'progress': 1}")
      }
      if (col.getObject("activity_id") == "do_course_unit3") {
        col.getObject("activity_type") should be("course-unit")
        println("aggMap", col.getObject("agg"))
        col.getObject("activity_type") should be("{'progress': 1}")
      }
      if (col.getObject("activity_id") == "do_1127212344324751361295") {
        col.getObject("activity_type") should be("course")
        println("aggMap", col.getObject("agg"))
        col.getObject("activity_type") should be("{'progress': 2}")
      }
    })


    val event2Progress = readFromCassandra(EventFixture.EVENT_2)
    event2Progress.size() should be(3)
    event2Progress.forEach(col => {
      if (col.getObject("activity_id") == "unit11") {
        col.getObject("activity_type") should be("course-unit")
        println("aggMap", col.getObject("agg"))
        col.getObject("activity_type") should be("{'progress': 2}")
      }
      if (col.getObject("activity_id") == "unit22") {
        col.getObject("activity_type") should be("course-unit")
        println("aggMap", col.getObject("agg"))
        col.getObject("activity_type") should be("{'progress': 1}")
      }
      if (col.getObject("activity_id") == "C11") {
        col.getObject("activity_type") should be("course")
        println("aggMap", col.getObject("agg"))
        col.getObject("activity_type") should be("{'progress': 2}")
      }
    })


    val event3Progress = readFromCassandra(EventFixture.EVENT_3)
    event3Progress.size() should be(3)
    event3Progress.forEach(col => {
      if (col.getObject("activity_id") == "unit1") {
        col.getObject("activity_type") should be("course-unit")
        println("aggMap", col.getObject("agg"))
        col.getObject("activity_type") should be("{'progress': 2}")
      }
      if (col.getObject("activity_id") == "unit2") {
        col.getObject("activity_type") should be("course-unit")
        println("aggMap", col.getObject("agg"))
        col.getObject("activity_type") should be("{'progress': 2}")
      }
      if (col.getObject("activity_id") == "course001") {
        col.getObject("activity_type") should be("course")
        println("aggMap", col.getObject("agg"))
        col.getObject("activity_type") should be("{'progress': 3}")
      }
    })

  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }

  def updateRedis(jedis: Jedis, testData: Map[String, AnyRef]) {
    testData.get("cacheData").map(data => {
      data.asInstanceOf[List[Map[String, AnyRef]]].map(cacheData => {
        cacheData.map(x => {
          x._2.asInstanceOf[List[String]].foreach(d => {
            jedis.lpush(x._1, d)
          })
        })
      })
    })
  }

  def readFromCassandra(event: String): util.List[Row] = {
    val event1_primaryCols = getPrimaryCols(gson.fromJson(event, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala.asJava)
    val query = s"select * from sunbird_courses.activity_agg where context_id='cb:${event1_primaryCols.get("batchid").get}' and user_id='${event1_primaryCols.get("userid").get}' ALLOW FILTERING;"
    cassandraUtil.find(query)
  }


  def getPrimaryCols(event: util.Map[String, AnyRef]): mutable.Map[String, String] = {
    val eventData = event.get("edata").asInstanceOf[util.Map[String, AnyRef]]
    eventData.asScala.map(v => (v._1.toLowerCase, v._2)).filter(x => courseAggregatorConfig.primaryFields.contains(x._1)).asInstanceOf[mutable.Map[String, String]]
  }
}


class CourseAggregatorMapSource extends SourceFunction[util.Map[String, AnyRef]] {

  override def run(ctx: SourceContext[util.Map[String, AnyRef]]) {
    val gson = new Gson()
    val eventMap1 = gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala ++ Map("partition" -> 0.asInstanceOf[AnyRef])
    val eventMap2 = gson.fromJson(EventFixture.EVENT_2, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala ++ Map("partition" -> 1.asInstanceOf[AnyRef])
    val eventMap3 = gson.fromJson(EventFixture.EVENT_3, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala ++ Map("partition" -> 0.asInstanceOf[AnyRef])
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