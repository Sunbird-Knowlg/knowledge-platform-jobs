package org.sunbird.job.aggregate.functions

import java.lang.reflect.Type
import java.security.MessageDigest
import java.util

import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.RedisConnect
import org.sunbird.job.aggregate.common.DeDupHelper
import org.sunbird.job.dedup.DeDupEngine
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.aggregate.task.ActivityAggregateUpdaterConfig

import scala.collection.JavaConverters._

class ContentConsumptionDeDupFunction(config: ActivityAggregateUpdaterConfig)(implicit val stringTypeInfo: TypeInformation[String]) extends BaseProcessFunction[util.Map[String, AnyRef], String](config) {

  val mapType: Type = new TypeToken[Map[String, AnyRef]]() {}.getType
  private[this] val logger = LoggerFactory.getLogger(classOf[ContentConsumptionDeDupFunction])
  var deDupEngine: DeDupEngine = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    deDupEngine = new DeDupEngine(config, new RedisConnect(config, Option(config.deDupRedisHost), Option(config.deDupRedisPort)), config.deDupStore, config.deDupExpirySec)
    deDupEngine.init()
  }

  override def close(): Unit = {
    deDupEngine.close()
    super.close()
  }

  override def processElement(event: util.Map[String, AnyRef], context: ProcessFunction[util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventCount)
    val eData = event.get(config.eData).asInstanceOf[util.Map[String, AnyRef]].asScala
    val isBatchEnrollmentEvent: Boolean = StringUtils.equalsIgnoreCase(eData.getOrElse(config.action, "").asInstanceOf[String], config.batchEnrolmentUpdateCode)
    if (isBatchEnrollmentEvent) {
      val contents = eData.getOrElse(config.contents, new util.ArrayList[java.util.Map[String, AnyRef]]()).asInstanceOf[util.List[java.util.Map[String, AnyRef]]].asScala
      val filteredContents = contents.filter(x => x.get("status") == 2).toList
      if (filteredContents.size == 0)
        metrics.incCounter(config.skipEventsCount)
      else
        metrics.incCounter(config.batchEnrolmentUpdateEventCount)
      filteredContents.map(c => {
        (eData + ("contents" -> List(Map("contentId" -> c.get("contentId"), "status" -> c.get("status"))))).toMap
      }).filter(e => discardDuplicates(e)).foreach(d => context.output(config.uniqueConsumptionOutput, d))
    } else metrics.incCounter(config.skipEventsCount)
  }

  override def metricsList(): List[String] = {
    List(config.totalEventCount, config.skipEventsCount, config.batchEnrolmentUpdateEventCount)
  }

  def discardDuplicates(event: Map[String, AnyRef]): Boolean = {
    if (config.dedupEnabled) {
      val userId = event.getOrElse(config.userId, "").asInstanceOf[String]
      val courseId = event.getOrElse(config.courseId, "").asInstanceOf[String]
      val batchId = event.getOrElse(config.batchId, "").asInstanceOf[String]
      val contents = event.getOrElse(config.contents, List[Map[String,AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
      if (contents.nonEmpty) {
        val content = contents.head
        val contentId = content.getOrElse("contentId", "").asInstanceOf[String]
        val status = content.getOrElse("status", 0.asInstanceOf[AnyRef]).asInstanceOf[Number].intValue()
        val checksum = DeDupHelper.getMessageId(courseId, batchId, userId, contentId, status)
        deDupEngine.isUniqueEvent(checksum)
      } else false
    } else true
  }
}
