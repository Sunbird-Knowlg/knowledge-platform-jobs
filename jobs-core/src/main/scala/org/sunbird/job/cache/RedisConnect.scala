package org.sunbird.job.cache

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import org.sunbird.job.BaseJobConfig
import redis.clients.jedis.Jedis

class RedisConnect(jobConfig: BaseJobConfig, host: Option[String] = None, port: Option[Int] = None) extends java.io.Serializable {

  private val serialVersionUID = -396824011996012513L

  val config: Config = jobConfig.config
  val redisHost: String = host.getOrElse(Option(config.getString("redis.host")).getOrElse("localhost"))
  val redisPort: Int = port.getOrElse(Option(config.getInt("redis.port")).getOrElse(6379))
  private val logger = LoggerFactory.getLogger(classOf[RedisConnect])

  private def getConnection(backoffTimeInMillis: Long): Jedis = {
    val defaultTimeOut = 30000
    if (backoffTimeInMillis > 0) try Thread.sleep(backoffTimeInMillis)
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
    logger.info("Obtaining new Redis connection...")
    new Jedis(redisHost, redisPort, defaultTimeOut)
  }


  def getConnection(db: Int, backoffTimeInMillis: Long): Jedis = {
    val jedis: Jedis = getConnection(backoffTimeInMillis)
    jedis.select(db)
    jedis
  }

  def getConnection(db: Int): Jedis = {
    val jedis = getConnection(db, backoffTimeInMillis = 0)
    jedis.select(db)
    jedis
  }

  def getConnection: Jedis = getConnection(db = 0)
}
