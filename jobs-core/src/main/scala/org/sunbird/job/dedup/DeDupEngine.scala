package org.sunbird.job.dedup

import org.slf4j.LoggerFactory
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.cache.{DataCache, RedisConnect}
import redis.clients.jedis.Jedis

class DeDupEngine(val config: BaseJobConfig, val redisConnect: RedisConnect, val store: Int, val expirySeconds: Int) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DataCache])
  private var redisConnection: Jedis = _

  def init() {
    if (!config.redisEnabled) {
      logger.info("Deduplication is disabled: Redis is not enabled. All events will be processed.")
      return
    }
    this.redisConnection = redisConnect.getConnection(store)
  }

  def close() {
    if (!config.redisEnabled) return
    this.redisConnection.close()
  }

  import redis.clients.jedis.exceptions.JedisException

  def isUniqueEvent(checksum: String): Boolean = {
    if (!config.redisEnabled) return true
    try !redisConnection.exists(checksum)
    catch {
      case ex: JedisException =>
        this.redisConnection.close
        this.redisConnection = redisConnect.getConnection(store, 10000)
        !redisConnection.exists(checksum)
    }
  }

  def storeChecksum(checksum: String): Unit = {
    if (!config.redisEnabled) return
    try redisConnection.setex(checksum, expirySeconds, "")
    catch {
      case ex: JedisException =>
        this.redisConnection.close
        this.redisConnection = redisConnect.getConnection(store,10000)
        this.redisConnection.select(store)
        redisConnection.setex(checksum, expirySeconds, "")
    }
  }

}
