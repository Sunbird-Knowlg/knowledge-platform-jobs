package org.sunbird.spec

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.scalatest.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.fixture.EventFixture
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.serde.{MapDeserializationSchema, MapSerializationSchema, StringDeserializationSchema, StringSerializationSchema}
import org.sunbird.job.util.FlinkUtil
import redis.clients.jedis.exceptions.JedisDataException

class CoreTestSpec extends BaseSpec with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("base-test.conf")
  val baseConfig: BaseJobConfig = new BaseJobConfig(config, "base-job")

  "RedisConnect functionality" should "be able to connect to redis" in {
    assume(redisAvailable, "Embedded Redis unavailable on this platform, skipping")
    val redisConnection = new RedisConnect(baseConfig)
    val status = redisConnection.getConnection(2)
    status.isConnected should be(true)
  }



  "DataCache hgetAllWithRetry function" should "be able to retrieve the map data from Redis" in {
    assume(redisAvailable, "Embedded Redis unavailable on this platform, skipping")
    val redisConnection = new RedisConnect(baseConfig)
    val map = new util.HashMap[String, String]()
    map.put("country_code", "IN")
    map.put("country", "INDIA")
    redisConnection.getConnection(2).hmset("5d8947598347589fsdlh43y8", map)
    redisConnection.getConnection(2).set("56934dufhksjd8947hdskj", "{\"actor\":{\"type\":\"User\",\"id\":\"4c4530df-0d4f-42a5-bd91-0366716c8c24\"}}")

    val deviceFields = List("country_code", "country", "state_code", "st" +
      "ate", "city", "district_custom", "state_code_custom",
      "state_custom", "user_declared_state", "user_declared_district", "devicespec", "firstaccess")
    val dataCache = new DataCache(baseConfig, new RedisConnect(baseConfig), 2, deviceFields)
    dataCache.init()
    val hGetResponse = dataCache.hgetAllWithRetry("5d8947598347589fsdlh43y8")
    val hGetResponse2 = dataCache.getMultipleWithRetry(List("56934dufhksjd8947hdskj"))
    hGetResponse2.size should be(1)
    hGetResponse.size should be(2)
    hGetResponse("country_code") should be("IN")
    hGetResponse("country") should be("INDIA")
    dataCache.close()
  }



  "StringSerialization functionality" should "be able to serialize the input data as String" in {
    val topic: String = "topic-test"
    val partition: Int = 0
    val offset: Long = 1
    val group: String = "group-test"
    val key: Array[Byte] = null
    val value: Array[Byte] = Array[Byte](1)
    val stringDeSerialization = new StringDeserializationSchema()
    val stringSerialization = new StringSerializationSchema(topic, Some("kafka-key"))
    val mapSerialization: MapSerializationSchema = new MapSerializationSchema(topic, Some("kafka-key"))
    val mapDeSerialization = new MapDeserializationSchema()
    import org.apache.kafka.clients.consumer.ConsumerRecord
    val cRecord: ConsumerRecord[Array[Byte], Array[Byte]] = new ConsumerRecord[Array[Byte], Array[Byte]](topic, partition, offset, key, value)
    val stringCollector = new Collector[String] {
      override def collect(record: String): Unit = {}
      override def close(): Unit = {}
    }
    stringDeSerialization.deserialize(cRecord, stringCollector)
    stringSerialization.serialize("test", null, System.currentTimeMillis())
    val map = new util.HashMap[String, AnyRef]()
    map.put("country_code", "IN")
    map.put("country", "INDIA")
    mapSerialization.serialize(map, null, System.currentTimeMillis())
  }

  "DataCache" should "be able to add the data into redis" in {
    assume(redisAvailable, "Embedded Redis unavailable on this platform, skipping")
    intercept[JedisDataException] {
      val redisConnection = new RedisConnect(baseConfig)
      val deviceData = new util.HashMap[String, String]()
      deviceData.put("country_code", "IN")
      deviceData.put("country", "INDIA")

      val deviceFields = List("country_code", "country", "state_code", "st" +
        "ate", "city", "district_custom", "state_code_custom",
        "state_custom", "user_declared_state", "user_declared_district", "devicespec", "firstaccess")
      val dataCache = new DataCache(baseConfig, redisConnection, 2, deviceFields)
      dataCache.init()
      dataCache.isExists("device_10") should be(false)
      dataCache.hmSet("device_1", deviceData)
      val redisData = dataCache.hgetAllWithRetry("device_1")
      redisData.size should be(2)
      redisData should not be (null)
      redisConnection.getConnection(2).close()
      dataCache.hmSet("device_1", deviceData)
      dataCache.getWithRetry(null)
    }
  }


  "DataCache" should "thorw an jedis exception when invalid action happen" in {
    assume(redisAvailable, "Embedded Redis unavailable on this platform, skipping")
    intercept[JedisDataException] {
      val redisConnection = new RedisConnect(baseConfig)
      val dataCache = new DataCache(baseConfig, redisConnection, 2, List())
      dataCache.init()
      dataCache.hgetAllWithRetry(null)
    }
  }

  "DataCache setWithRetry function" should "be able to set the data from Redis" in {
    assume(redisAvailable, "Embedded Redis unavailable on this platform, skipping")
    val redisConnection = new RedisConnect(baseConfig)
    val dataCache = new DataCache(baseConfig, redisConnection, 4, List("identifier"))
    dataCache.init()
    dataCache.setWithRetry("key", "{\"test\": \"value\"}")
    redisConnection.getConnection(4).get("key") should equal("{\"test\": \"value\"}")
  }


  "FilnkUtil" should "get the flink util context" in {
    val config = ConfigFactory.empty()
    config.entrySet()

    val customConf = ConfigFactory.parseString(EventFixture.customConfig)

    val flinkConfig: BaseJobConfig = new BaseJobConfig(customConf, "base-job")
    val context: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(flinkConfig)
    context should not be (null)
  }

}

