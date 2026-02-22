package org.sunbird.spec

import java.nio.charset.StandardCharsets
import java.util

import com.google.gson.Gson
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.serde._

import scala.collection.mutable.ListBuffer

class SerdeSpec extends FlatSpec with Matchers {

  // --- StringDeserializationSchema ---

  "StringDeserializationSchema" should "deserialize a record to string" in {
    val schema = new StringDeserializationSchema()
    val record = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 0, 1L, null, "hello world".getBytes(StandardCharsets.UTF_8))
    val results = ListBuffer[String]()
    val collector = new Collector[String] {
      override def collect(r: String): Unit = results += r
      override def close(): Unit = {}
    }
    schema.deserialize(record, collector)
    results.size should be(1)
    results.head should be("hello world")
  }

  it should "return correct produced type" in {
    val schema = new StringDeserializationSchema()
    val typeInfo = schema.getProducedType
    typeInfo should not be null
    typeInfo.getTypeClass should be(classOf[String])
  }

  // --- StringSerializationSchema with key ---

  "StringSerializationSchema" should "serialize with a key" in {
    val schema = new StringSerializationSchema("test-topic", Some("my-key"))
    val record = schema.serialize("test-value", null, System.currentTimeMillis())
    record should not be null
    record.topic() should be("test-topic")
    new String(record.key(), StandardCharsets.UTF_8) should be("my-key")
    new String(record.value(), StandardCharsets.UTF_8) should be("test-value")
  }

  it should "serialize without a key" in {
    val schema = new StringSerializationSchema("test-topic", None)
    val record = schema.serialize("test-value", null, System.currentTimeMillis())
    record should not be null
    record.topic() should be("test-topic")
    record.key() should be(null)
    new String(record.value(), StandardCharsets.UTF_8) should be("test-value")
  }

  it should "serialize with default key (None)" in {
    val schema = new StringSerializationSchema("test-topic")
    val record = schema.serialize("test-value", null, System.currentTimeMillis())
    record should not be null
    record.key() should be(null)
  }

  // --- MapDeserializationSchema ---

  "MapDeserializationSchema" should "deserialize a JSON record to Map" in {
    val schema = new MapDeserializationSchema()
    val json = """{"name":"test","value":"123"}"""
    val record = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 2, 5L, null, json.getBytes(StandardCharsets.UTF_8))
    val results = ListBuffer[util.Map[String, AnyRef]]()
    val collector = new Collector[util.Map[String, AnyRef]] {
      override def collect(r: util.Map[String, AnyRef]): Unit = results += r
      override def close(): Unit = {}
    }
    schema.deserialize(record, collector)
    results.size should be(1)
    val resultMap = results.head
    resultMap.get("name") should be("test")
    resultMap.get("value") should be("123")
    resultMap.get("partition") should be(2)
  }

  it should "return correct produced type" in {
    val schema = new MapDeserializationSchema()
    val typeInfo = schema.getProducedType
    typeInfo should not be null
  }

  // --- MapSerializationSchema with key ---

  "MapSerializationSchema" should "serialize with a key" in {
    val schema = new MapSerializationSchema("test-topic", Some("map-key"))
    val map = new util.HashMap[String, AnyRef]()
    map.put("field1", "value1")
    val record = schema.serialize(map, null, System.currentTimeMillis())
    record should not be null
    record.topic() should be("test-topic")
    new String(record.key(), StandardCharsets.UTF_8) should be("map-key")
    val deserializedValue = new Gson().fromJson(new String(record.value(), StandardCharsets.UTF_8), classOf[util.Map[String, AnyRef]])
    deserializedValue.get("field1") should be("value1")
  }

  it should "serialize without a key" in {
    val schema = new MapSerializationSchema("test-topic", None)
    val map = new util.HashMap[String, AnyRef]()
    map.put("field1", "value1")
    val record = schema.serialize(map, null, System.currentTimeMillis())
    record should not be null
    record.topic() should be("test-topic")
    record.key() should be(null)
  }

  it should "serialize with default key (None)" in {
    val schema = new MapSerializationSchema("test-topic")
    val map = new util.HashMap[String, AnyRef]()
    map.put("data", "test")
    val record = schema.serialize(map, null, System.currentTimeMillis())
    record should not be null
    record.key() should be(null)
  }

  // --- JobRequestDeserializationSchema ---

  "JobRequestDeserializationSchema" should "deserialize a valid job request record" in {
    val schema = new JobRequestDeserializationSchema[TestJobRequest]()
    val json = """{"eid":"BE_JOB_REQUEST","ets":1608027159,"mid":"test-mid"}"""
    val record = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 1, 100L, null, json.getBytes(StandardCharsets.UTF_8))
    val results = ListBuffer[TestJobRequest]()
    val collector = new Collector[TestJobRequest] {
      override def collect(r: TestJobRequest): Unit = results += r
      override def close(): Unit = {}
    }
    schema.deserialize(record, collector)
    results.size should be(1)
    results.head should not be null
  }

  it should "handle invalid JSON gracefully and still produce a record" in {
    val schema = new JobRequestDeserializationSchema[TestJobRequest]()
    val record = new ConsumerRecord[Array[Byte], Array[Byte]]("topic", 0, 50L, null, "invalid-json{{{".getBytes(StandardCharsets.UTF_8))
    val results = ListBuffer[TestJobRequest]()
    val collector = new Collector[TestJobRequest] {
      override def collect(r: TestJobRequest): Unit = results += r
      override def close(): Unit = {}
    }
    schema.deserialize(record, collector)
    results.size should be(1)
    results.head should not be null
  }

  it should "return correct produced type" in {
    val schema = new JobRequestDeserializationSchema[TestJobRequest]()
    val typeInfo = schema.getProducedType
    typeInfo should not be null
  }

  // --- JobRequestSerializationSchema ---

  "JobRequestSerializationSchema" should "serialize a job request with mid (kafka key)" in {
    val schema = new JobRequestSerializationSchema[TestJobRequest]("test-topic")
    val map = new util.HashMap[String, Any]()
    map.put("eid", "BE_JOB_REQUEST")
    map.put("mid", "LP.1608027159.test-mid-123")
    val request = new TestJobRequest(map, 0, 0L)
    val record = schema.serialize(request, null, System.currentTimeMillis())
    record should not be null
    record.topic() should be("test-topic")
    record.value() should not be null
    record.key() should not be null
    new String(record.key(), StandardCharsets.UTF_8) should be("LP.1608027159.test-mid-123")
  }

  it should "serialize a job request without mid (null kafka key)" in {
    val schema = new JobRequestSerializationSchema[TestJobRequest]("test-topic")
    val map = new util.HashMap[String, Any]()
    map.put("eid", "BE_JOB_REQUEST")
    val request = new TestJobRequest(map, 0, 0L)
    val record = schema.serialize(request, null, System.currentTimeMillis())
    record should not be null
    record.topic() should be("test-topic")
    record.value() should not be null
    record.key() should be(null)
  }
}
