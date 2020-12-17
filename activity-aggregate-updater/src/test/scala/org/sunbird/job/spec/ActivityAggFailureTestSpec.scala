package org.sunbird.job.spec

import java.util

import com.google.gson.Gson
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito.when
import org.sunbird.job.cache.RedisConnect
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.task.ActivityAggregateUpdaterStreamTask
import org.sunbird.job.util.{CassandraUtil, HTTPResponse}
import org.sunbird.spec.BaseMetricsReporter

class ActivityAggFailureTestSpec extends BaseActivityAggregateTestSpec {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val redisConnect = new RedisConnect(courseAggregatorConfig)
    jedis = redisConnect.getConnection(courseAggregatorConfig.nodeStore)
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(courseAggregatorConfig.dbHost, courseAggregatorConfig.dbPort)
    val session = cassandraUtil.session

    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
    // Clear the metrics
    testCassandraUtil(cassandraUtil)
    BaseMetricsReporter.gaugeMetrics.clear()
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

  val requestBody = s"""{
                       |    "request": {
                       |        "filters": {
                       |            "objectType": "Collection",
                       |            "identifier": "course001",
                       |            "status": ["Live", "Unlisted", "Retired"]
                       |        },
                       |        "fields": ["status"]
                       |    }
                       |}""".stripMargin

  "ActivityAgg " should " throw exception when the cache not available for root collection" in {
    when(mockKafkaUtil.kafkaMapSource(courseAggregatorConfig.kafkaInputTopic)).thenReturn(new ActivityAggFailureInput1MapSource)
    when(mockKafkaUtil.kafkaStringSink(courseAggregatorConfig.kafkaAuditEventTopic)).thenReturn(new auditEventSink)
    when(mockKafkaUtil.kafkaStringSink(courseAggregatorConfig.kafkaFailedEventTopic)).thenReturn(new failedEventSink)
    when(mockKafkaUtil.kafkaStringSink(courseAggregatorConfig.kafkaCertIssueTopic)).thenReturn(new certificateIssuedEventsSink)
    when(mockHttpUtil.post(courseAggregatorConfig.searchAPIURL, requestBody)).thenReturn(new HTTPResponse(200, """{"id":"api.v1.search","ver":"1.0","ts":"2020-12-16T12:37:40.283Z","params":{"resmsgid":"7c4cf0b0-3f9b-11eb-9b0c-abcfbdf41bc3","msgid":"7c4b1bf0-3f9b-11eb-9b0c-abcfbdf41bc3","status":"successful","err":null,"errmsg":null},"responseCode":"OK","result":{"count":1,"content":[{"identifier":"course001","objectType":"Content","status":"Live"}]}}"""))

    val activityAggTask = new ActivityAggregateUpdaterStreamTask(courseAggregatorConfig, mockKafkaUtil, mockHttpUtil)
    the [Exception] thrownBy {
      activityAggTask.process()
    } should have message "Job execution failed."

    // De-dup should not save the keys for which the processing failed.
    // This will help in processing the same data after restart.
    jedis.select(courseAggregatorConfig.deDupStore)
    jedis.keys("*").size() should be (0)

    failedEventSink.values.forEach(event => {
      println("FAILED_EVENT_DATA: " + event)
    })
    failedEventSink.values.size() should be (2)

  }
}

private class ActivityAggFailureInput1MapSource extends SourceFunction[util.Map[String, AnyRef]] {

  override def run(ctx: SourceContext[util.Map[String, AnyRef]]) {
    ctx.collect(jsonToMap(EventFixture.CC_EVENT1))
    ctx.collect(jsonToMap(EventFixture.CC_EVENT2))
    ctx.collect(jsonToMap(EventFixture.CC_EVENT3))
  }

  override def cancel() = {}

  def jsonToMap(json: String): util.Map[String, AnyRef] = {
    val gson = new Gson()
    gson.fromJson(json, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
  }

}