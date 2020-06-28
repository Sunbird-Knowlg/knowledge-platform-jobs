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
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(courseAggregatorConfig.dbHost, courseAggregatorConfig.dbPort)
    val session = cassandraUtil.session


    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    // Clear the metrics
    testCassandraUtil(cassandraUtil)
    BaseMetricsReporter.gaugeMetrics.clear()
    cassandraUtil.upsert("TRUNCATE sunbird_courses.user_enrolments ;")
    jedis.flushDB()
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

    addLeafNodesToRedis(jedis, EventFixture.EVENT_1_LEAF_NODES)
    when(mockKafkaUtil.kafkaMapSource(courseAggregatorConfig.kafkaInputTopic)).thenReturn(new CourseAggregatorMapSource)
    when(mockKafkaUtil.kafkaStringSink(courseAggregatorConfig.kafkaFailedTopic)).thenReturn(new FailedEventsSink)
    //when(mockKafkaUtil.kafkaStringSink(courseAggregatorConfig.kafka)).thenReturn(new FailedEventsSink)
    new CourseAggregatorStreamTask(courseAggregatorConfig, mockKafkaUtil).process()
//    BaseMetricsReporter.gaugeMetrics(s"${courseAggregatorConfig.jobName}.${courseAggregatorConfig.totalEventsCount}").getValue() should be(1)
//    BaseMetricsReporter.gaugeMetrics(s"${courseAggregatorConfig.jobName}.${courseAggregatorConfig.dbUpdateCount}").getValue() should be(1)
//    BaseMetricsReporter.gaugeMetrics(s"${courseAggregatorConfig.jobName}.${courseAggregatorConfig.cacheHitCount}").getValue() should be(1)
//    BaseMetricsReporter.gaugeMetrics(s"${courseAggregatorConfig.jobName}.${courseAggregatorConfig.successEventCount}").getValue() should be(1)
   val event1_primaryCols = getPrimaryCols(gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala.asJava)
    // Cassandra Query IsBEGIN BATCH UPDATE sunbird_courses.user_enrolments SET progress=3,contentstatus={'do_11260735471149056012301':1,'do_11260735471149056012300':1,'do_11260735471149056012299':2},completionpercentage=300,completedon=null,status=1 WHERE batchid='0126083288437637121' AND userid='do_1127212344324751361295' AND courseid='8454cb21-3ce9-4e30-85b5-fade097880d8';APPLY BATCH;
    val queryIs = s"select progress,status,contentstatus,completedon,completionpercentage from sunbird_courses.user_enrolments where courseid='${event1_primaryCols.get("courseid").get}' and batchid='${event1_primaryCols.get("batchid").get}' and userid='${event1_primaryCols.get("userid").get}';"
    val event1Progress = cassandraUtil.findOne(queryIs)
    println("event1Progress" + event1Progress)
    event1Progress.getObject("progress") should be(1)
    event1Progress.getObject("status") should be(1)
    event1Progress.getObject("completionpercentage") should be(33)
    event1Progress.getObject("completedon") should be(null)
    event1Progress.getObject("contentstatus") should not be(null)
  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
    val response = cassandraUtil.find(s"SELECT * FROM ${courseAggregatorConfig.dbKeyspace}.${courseAggregatorConfig.dbTable};")
    println("cassandra resposne" + response)
    response should not be (null)
  }

  def addLeafNodesToRedis(jedis: Jedis, leafNodes: Map[String, List[String]]) {
    leafNodes.foreach(x => {
      x._2.foreach(y => {
        jedis.lpush(x._1, y)
      })

    })
    jedis.close()
  }

  def getPrimaryCols(event: util.Map[String, AnyRef]): mutable.Map[String, String] = {
    val eventData = event.get("edata").asInstanceOf[util.Map[String, AnyRef]]
    eventData.asScala.map(v => (v._1.toLowerCase, v._2)).filter(x => courseAggregatorConfig.primaryCols.contains(x._1)).asInstanceOf[mutable.Map[String, String]]
  }
}


class CourseAggregatorMapSource extends SourceFunction[util.Map[String, AnyRef]] {

  override def run(ctx: SourceContext[util.Map[String, AnyRef]]) {
    val gson = new Gson()
    val eventMap1 = gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala ++ Map("partition" -> 0.asInstanceOf[AnyRef])
    val eventMap2 = gson.fromJson(EventFixture.EVENT_2, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala ++ Map("partition" -> 1.asInstanceOf[AnyRef])
    ctx.collect(eventMap1.asJava)
    ctx.collect(eventMap2.asJava)
  }
  override def cancel() = {}

}


class FailedEventsSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      FailedEventsSink.values.add(value)
    }
  }
}

object FailedEventsSink {
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