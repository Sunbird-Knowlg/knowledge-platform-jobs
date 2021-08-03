package org.sunbird.job.cache

import java.util

import com.google.gson.Gson
import org.slf4j.LoggerFactory
import org.sunbird.job.BaseJobConfig
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.{JedisConnectionException, JedisException}

import scala.collection.JavaConverters._
import scala.collection.mutable.Map

class DataCache(val config: BaseJobConfig, val redisConnect: RedisConnect, val dbIndex: Int, val fields: List[String]) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DataCache])
  private var redisConnection: Jedis = _
  val gson = new Gson()

  def init() {
    this.redisConnection = redisConnect.getConnection(dbIndex)
  }

  def close() {
    this.redisConnection.close()
  }

  def hgetAllWithRetry(key: String): Map[String, String] = {
    try {
      hgetAll(key)
    } catch {
      case ex: JedisException =>
        logger.error("Exception when retrieving data from redis cache", ex)
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(dbIndex)
        hgetAll(key)
    }

  }

  private def hgetAll(key: String): Map[String, String] = {
    val dataMap = redisConnection.hgetAll(key)
    if (dataMap.size() > 0) {
      dataMap.keySet().retainAll(fields.asJava)
      dataMap.values().removeAll(util.Collections.singleton(""))
      dataMap.asScala
    } else {
      Map[String, String]()
    }
  }

  def getWithRetry(key: String): Map[String, AnyRef] = {
    try {
      get(key)
    } catch {
      case ex: JedisException =>
        logger.error("Exception when retrieving data from redis cache", ex)
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(dbIndex)
        get(key)
    }

  }

  private def get(key: String): Map[String, AnyRef] = {
    val data = redisConnection.get(key)
    if (data != null && !data.isEmpty()) {
      val dataMap = gson.fromJson(data, new util.HashMap[String, AnyRef]().getClass)
      if(fields.nonEmpty)
        dataMap.keySet().retainAll(fields.asJava)
      dataMap.values().removeAll(util.Collections.singleton(""))
      val map = dataMap.asScala
      map.map(f => {
        (f._1.toLowerCase().replace("_", ""), f._2)
      })
    } else {
      Map[String, AnyRef]()
    }
  }

  def getMultipleWithRetry(keys: List[String]): List[Map[String, AnyRef]] = {
    for (key <- keys) yield {
      getWithRetry(key)
    }
  }

  def isExists(key: String): Boolean = {
    redisConnection.exists(key)
  }

  def hmSet(key: String, value: util.Map[String, String]): Unit = {
    try {
      redisConnection.hmset(key, value)
    } catch {
      // Write testcase for catch block
      // $COVERAGE-OFF$ Disabling scoverage
      case ex: JedisException => {
        println("dataCache")
        logger.error("Exception when inserting data to redis cache", ex)
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(dbIndex)
        this.redisConnection.hmset(key, value)
      }
    }
  }

  /**
   * The cache will be created by clearing the existing data from smembers.
   * @param key
   * @param value
   */
  def createListWithRetry(key: String, value: List[String]): Unit = {
    try {
      redisConnection.del(key)
      redisConnection.sadd(key, value.map(_.asInstanceOf[String]): _*)
    } catch {
      // Write testcase for catch block
      // $COVERAGE-OFF$ Disabling scoverage
      case ex: JedisException => {
        logger.error("Exception when inserting data to redis cache", ex)
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(dbIndex)
        redisConnection.del(key)
        redisConnection.sadd(key, value.map(_.asInstanceOf[String]): _*)
      }
    }
  }

  /**
   * The cache will add the given members if already exists otherwise, it will create cache with the given members.
   * @param key
   * @param value
   */
  def addListWithRetry(key: String, value: List[String]): Unit = {
    try {
      redisConnection.sadd(key, value.map(_.asInstanceOf[String]): _*)
    } catch {
      // Write testcase for catch block
      // $COVERAGE-OFF$ Disabling scoverage
      case ex: JedisException => {
        logger.error("Exception when inserting data to redis cache", ex)
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(dbIndex)
        redisConnection.sadd(key, value.map(_.asInstanceOf[String]): _*)
      }
    }
  }

  def setWithRetry(key: String, value: String): Unit = {
    try {
      set(key, value);
    } catch {
      case ex@(_: JedisConnectionException | _: JedisException) =>
        logger.error("Exception when update data to redis cache", ex)
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(dbIndex);
        set(key, value)
    }
  }

  def set(key: String, value: String): Unit = {
    redisConnection.set(key, value)
  }

  def sMembers(key: String): util.Set[String] = {
    redisConnection.smembers(key)
  }

  def getKeyMembers(key: String): util.Set[String] = {
    try {
      sMembers(key)
    } catch {
      case ex: JedisException =>
        logger.error("Exception when retrieving data from redis cache", ex)
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(dbIndex)
        sMembers(key)
    }
  }

  def del(key: String): Unit = {
    this.redisConnection.del(key)
  }

}

// $COVERAGE-ON$