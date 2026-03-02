package org.sunbird.spec

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.when
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

  val mockJedis = mock[redis.clients.jedis.Jedis]
  val mockRedisConnect = mock[RedisConnect]
  when(mockRedisConnect.getConnection(anyInt())).thenReturn(mockJedis)
  when(mockRedisConnect.getConnection).thenReturn(mockJedis)

  "RedisConnect functionality" should "be able to connect to redis" in {
    when(mockJedis.isConnected).thenReturn(true)
    val status = mockRedisConnect.getConnection(2)
    status.isConnected should be(true)
  }

  "DataCache hgetAllWithRetry function" should "be able to retrieve the map data from Redis" in {
    val map = new util.HashMap[String, String]()
    map.put("country_code", "IN")
    map.put("country", "INDIA")
    
    when(mockJedis.hgetAll("5d8947598347589fsdlh43y8")).thenReturn(map)
    val responseList = new util.ArrayList[String]()
    responseList.add("{\"actor\":{\"type\":\"User\",\"id\":\"4c4530df-0d4f-42a5-bd91-0366716c8c24\"}}")
    when(mockJedis.mget(any())).thenReturn(responseList)

    val deviceFields = List("country_code", "country", "state_code", "st" +
      "ate", "city", "district_custom", "state_code_custom",
      "state_custom", "user_declared_state", "user_declared_district", "devicespec", "firstaccess")
    val dataCache = new DataCache(baseConfig, mockRedisConnect, 2, deviceFields)
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
    stringDeSerialization.deserialize(cRecord)
    stringSerialization.serialize("test", System.currentTimeMillis())
    stringDeSerialization.isEndOfStream("") should be(false)
    val map = new util.HashMap[String, AnyRef]()
    map.put("country_code", "IN")
    map.put("country", "INDIA")
    mapSerialization.serialize(map, System.currentTimeMillis())
  }

  "DataCache" should "be able to add the data into redis" in {
    val deviceData = new util.HashMap[String, String]()
    deviceData.put("country_code", "IN")
    deviceData.put("country", "INDIA")

    when(mockJedis.exists("device_10")).thenReturn(false)
    when(mockJedis.hgetAll("device_1")).thenReturn(deviceData)

    val deviceFields = List("country_code", "country", "state_code", "st" +
      "ate", "city", "district_custom", "state_code_custom",
      "state_custom", "user_declared_state", "user_declared_district", "devicespec", "firstaccess")
    val dataCache = new DataCache(baseConfig, mockRedisConnect, 2, deviceFields)
    dataCache.init()
    dataCache.isExists("device_10") should be(false)
    dataCache.hmSet("device_1", deviceData)
    val redisData = dataCache.hgetAllWithRetry("device_1")
    redisData.size should be(2)
    redisData should not be (null)
  }


  "DataCache" should "throw an jedis exception when invalid action happen" in {
    val dataCache = new DataCache(baseConfig, mockRedisConnect, 2, List())
    dataCache.init()
    when(mockJedis.hgetAll(org.mockito.ArgumentMatchers.isNull())).thenThrow(new JedisDataException("Invalid key"))
    intercept[JedisDataException]{
      dataCache.hgetAllWithRetry(null)
    }
  }

  "DataCache setWithRetry function" should "be able to set the data from Redis" in {
    val dataCache = new DataCache(baseConfig, mockRedisConnect, 4, List("identifier"))
    dataCache.init()
    when(mockJedis.get("key")).thenReturn("{\"test\": \"value\"}")
    dataCache.setWithRetry("key", "{\"test\": \"value\"}")
    mockRedisConnect.getConnection(4).get("key") should equal("{\"test\": \"value\"}")
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

