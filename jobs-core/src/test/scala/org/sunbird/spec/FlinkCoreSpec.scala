package org.sunbird.spec

import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetResetStrategy}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.sunbird.fixture.EventFixture
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.serde.{JobRequestDeserializationSchema, MapDeserializationSchema}
import org.sunbird.job.util.FlinkUtil

/**
 * Tests for FlinkUtil, FlinkKafkaConnector, and serde classes.
 */
class FlinkCoreSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  // -----------------------------------------------------------------------
  // FlinkUtil
  // -----------------------------------------------------------------------

  "FlinkUtil" should "configure distributed checkpointing when enabled" in {
    val conf = ConfigFactory.parseString(EventFixture.customConfig)
    val flinkConfig = new BaseJobConfig(conf, "base-job")
    val env = FlinkUtil.getExecutionContext(flinkConfig)
    env should not be null
    env.getCheckpointConfig.isExternalizedCheckpointsEnabled should be(true)
    env.getCheckpointConfig.getMinPauseBetweenCheckpoints should be(30000L)
  }

  it should "not enable externalized checkpoints when distributed checkpointing is absent" in {
    val conf = ConfigFactory.parseString(
      """kafka { broker-servers = "localhost:9093", groupId = "test" }
        |task { restart-strategy.attempts = 1, restart-strategy.delay = 1000, parallelism = 1,
        |  consumer.parallelism = 1, checkpointing.compressed = false, checkpointing.interval = 60000,
        |  checkpointing.pause.between.seconds = 30000 }
        |lms-cassandra { host = "localhost", port = 9042 }
        |""".stripMargin)
    val flinkConfig = new BaseJobConfig(conf, "base-job")
    val env = FlinkUtil.getExecutionContext(flinkConfig)
    env should not be null
    env.getCheckpointConfig.isExternalizedCheckpointsEnabled should be(false)
  }

  // -----------------------------------------------------------------------
  // FlinkKafkaConnector — offset reset strategy
  // -----------------------------------------------------------------------

  "FlinkKafkaConnector" should "resolve 'earliest' to EARLIEST" in {
    FlinkKafkaConnector.resolveOffsetResetStrategy(Some("earliest")) should be(OffsetResetStrategy.EARLIEST)
  }

  it should "resolve 'latest' to LATEST" in {
    FlinkKafkaConnector.resolveOffsetResetStrategy(Some("latest")) should be(OffsetResetStrategy.LATEST)
  }

  it should "resolve config value case-insensitively" in {
    FlinkKafkaConnector.resolveOffsetResetStrategy(Some("EARLIEST")) should be(OffsetResetStrategy.EARLIEST)
  }

  it should "default to LATEST when config key is absent" in {
    FlinkKafkaConnector.resolveOffsetResetStrategy(None) should be(OffsetResetStrategy.LATEST)
  }

  it should "default to LATEST for an unrecognised value" in {
    FlinkKafkaConnector.resolveOffsetResetStrategy(Some("invalid")) should be(OffsetResetStrategy.LATEST)
  }

  // -----------------------------------------------------------------------
  // MapDeserializationSchema
  // -----------------------------------------------------------------------

  "MapDeserializationSchema" should "deserialize a record and inject the kafka partition into the map" in {
    val payload = """{"key":"value","count":1}""".getBytes("UTF-8")
    val record  = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 3, 10L, null, payload)

    val out    = new java.util.ArrayList[util.Map[String, AnyRef]]()
    val schema = new MapDeserializationSchema()
    schema.deserialize(record, new Collector[util.Map[String, AnyRef]] {
      override def collect(t: util.Map[String, AnyRef]): Unit = out.add(t)
      override def close(): Unit = ()
    })

    out.size()              should be(1)
    out.get(0).get("key")       should be("value")
    out.get(0).get("partition") should be(3)
  }

  // -----------------------------------------------------------------------
  // JobRequestDeserializationSchema
  // -----------------------------------------------------------------------

  "JobRequestDeserializationSchema" should "deserialize a valid JSON event" in {
    val payload = """{"eid":"BE_JOB_REQUEST","action":"publish"}""".getBytes("UTF-8")
    val record  = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 1, 5L, null, payload)

    val out    = new java.util.ArrayList[TestJobRequest]()
    val schema = new JobRequestDeserializationSchema[TestJobRequest]()
    schema.deserialize(record, new Collector[TestJobRequest] {
      override def collect(t: TestJobRequest): Unit = out.add(t)
      override def close(): Unit = ()
    })

    out.size()                       should be(1)
    out.get(0).getMap.get("eid")     should be("BE_JOB_REQUEST")
    out.get(0).getMap.get("action")  should be("publish")
  }

  it should "emit an empty event on malformed JSON without throwing" in {
    val record = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 0, 0L, null,
      "not-valid-json{{".getBytes("UTF-8"))

    val out    = new java.util.ArrayList[TestJobRequest]()
    val schema = new JobRequestDeserializationSchema[TestJobRequest]()
    noException should be thrownBy schema.deserialize(record, new Collector[TestJobRequest] {
      override def collect(t: TestJobRequest): Unit = out.add(t)
      override def close(): Unit = ()
    })
    out.size()                 should be(1)
    out.get(0).getMap.isEmpty  should be(true)
  }
}
