package org.sunbird.job.dedup

import org.slf4j.LoggerFactory
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.cache.{DataCache, RedisConnect}
import redis.clients.jedis.Jedis

class DeDupEngine(val config: BaseJobConfig, val redisConnect: RedisConnect, val store: Int, val expirySeconds: Int) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DataCache])
  private var redisConnection: Jedis = _

  def init() {
    this.redisConnection = redisConnect.getConnection(store)
  }

  def close() {
    this.redisConnection.close()
  }

  import redis.clients.jedis.exceptions.JedisException

  def isUniqueEvent(checksum: String): Boolean = {
    try !redisConnection.exists(checksum)
    catch {
      case ex: JedisException =>
        this.redisConnection.close
        this.redisConnection = redisConnect.getConnection(store, 10000)
        !redisConnection.exists(checksum)
    }
  }

  def storeChecksum(checksum: String): Unit = {
    try redisConnection.setex(checksum, expirySeconds, "")
    catch {
      case ex: JedisException =>
        this.redisConnection.close
        this.redisConnection = redisConnect.getConnection(10000)
        this.redisConnection.select(store)
        redisConnection.setex(checksum, expirySeconds, "")
    }
  }

}
