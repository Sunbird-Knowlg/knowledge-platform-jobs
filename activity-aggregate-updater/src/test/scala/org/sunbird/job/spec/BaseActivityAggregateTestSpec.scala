package org.sunbird.job.spec

import java.util

import com.datastax.driver.core.Row
import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.Mockito
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.task.{ActivityAggregateUpdaterConfig, ActivityAggregateUpdaterStreamTask}
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.spec.BaseTestSpec
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import scala.collection.JavaConverters._
import scala.collection.mutable

class BaseActivityAggregateTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  var redisServer: RedisServer = _
  redisServer = new RedisServer(6340)
  redisServer.start()
  var jedis: Jedis = _
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val gson = new Gson()
  val config: Config = ConfigFactory.load("test.conf")
  val courseAggregatorConfig: ActivityAggregateUpdaterConfig = new ActivityAggregateUpdaterConfig(config)

  var cassandraUtil: CassandraUtil = _
  val mockHttpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }

  def updateRedis(jedis: Jedis, testData: Map[String, AnyRef]) {
    testData.get("cacheData").map(data => {
      data.asInstanceOf[List[Map[String, AnyRef]]].map(cacheData => {
        cacheData.map(x => {
          x._2.asInstanceOf[List[String]].foreach(d => {
            jedis.sadd(x._1, d)
          })
        })
      })
    })
  }

  def readFromCassandra(event: String): util.List[Row] = {
    val event1_primaryCols = getPrimaryCols(gson.fromJson(event, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala.asJava)
    val query = s"select * from sunbird_courses.user_activity_agg where context_id='cb:${event1_primaryCols.get("batchid").get}' and user_id='${event1_primaryCols.get("userid").get}' ALLOW FILTERING;"
    cassandraUtil.find(query)
  }

  def readFromContentConsumptionTable(event: String): util.List[Row] = {
    val event1_primaryCols = getPrimaryCols(gson.fromJson(event, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]].asScala.asJava)
    val query = s"select * from sunbird_courses.user_content_consumption where userid='${event1_primaryCols.get("userid").get}' and batchid='${event1_primaryCols.get("batchid").get}' and courseid='${event1_primaryCols.get("courseid").get}' ALLOW FILTERING;"
    cassandraUtil.find(query)
  }


  def getPrimaryCols(event: util.Map[String, AnyRef]): mutable.Map[String, String] = {
    val eventData = event.get("edata").asInstanceOf[util.Map[String, AnyRef]]
    val primaryFields = List("userid", "courseid", "batchid")
    eventData.asScala.map(v => (v._1.toLowerCase, v._2)).filter(x => primaryFields.contains(x._1)).asInstanceOf[mutable.Map[String, String]]
  }

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

class failedEventSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      failedEventSink.values.add(value)
    }
  }
}

object failedEventSink {
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


class certificateIssuedEventsSink extends SinkFunction[String] {

  override def invoke(value: String): Unit = {
    synchronized {
      certificateIssuedEvents.values.add(value)
    }
  }
}

object certificateIssuedEvents {
  val values: util.List[String] = new util.ArrayList()
}
