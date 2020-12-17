package org.sunbird.job.functions

import java.lang.reflect.Type
import java.security.MessageDigest
import java.util

import com.google.gson.reflect.TypeToken
import org.apache.flink.configuration.Configuration
import org.sunbird.job.BaseProcessFunction
import org.sunbird.job.cache.RedisConnect
import org.sunbird.job.dedup.DeDupEngine
import org.sunbird.job.task.ActivityAggregateUpdaterConfig

abstract class BaseCCDeDupFunction(config: ActivityAggregateUpdaterConfig) extends BaseProcessFunction[util.Map[String, AnyRef], String](config) {

  val mapType: Type = new TypeToken[Map[String, AnyRef]]() {}.getType
  protected var deDupEngine: DeDupEngine = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    deDupEngine = new DeDupEngine(config, new RedisConnect(config, Option(config.deDupRedisHost), Option(config.deDupRedisPort)), config.deDupStore, config.deDupExpirySec)
    deDupEngine.init()
  }

  override def close(): Unit = {
    deDupEngine.close()
    super.close()
  }

  def getMessageId(collectionId: String, batchId: String, userId: String, contentId: String, status: Int): String = {
    val key = Array(collectionId, batchId, userId, contentId, status).mkString("|")
    MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
  }

}
