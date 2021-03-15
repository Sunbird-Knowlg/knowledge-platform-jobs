package org.sunbird.job.functions

import com.datastax.driver.core.querybuilder.{QueryBuilder, Update}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.twitter.storehaus.cache.TTLCache
import com.twitter.util.Duration
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.domain.{CollectionProgress, UserActivityAgg}
import org.sunbird.job.task.EnrolmentReconciliationConfig
import org.sunbird.job.util.CassandraUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._


class EnrolmentReconciliationFn(config: EnrolmentReconciliationConfig)
                               (implicit val stringTypeInfo: TypeInformation[String],
                                @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[EnrolmentReconciliationFn])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  private var cache: DataCache = _
  private var collectionStatusCache: TTLCache[String, String] = _
  lazy private val gson = new Gson()


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    cache = new DataCache(config, new RedisConnect(config), config.nodeStore, List())
    cache.init()
    collectionStatusCache = TTLCache[String, String](Duration.apply(config.statusCacheExpirySec, TimeUnit.SECONDS))

  }

  override def close(): Unit = {
    cassandraUtil.close()
    cache.close()
    super.close()
  }

  override def metricsList(): List[String] = {
    List()
  }

  override def processElement(event: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    val eData = event.asScala.get("edata").asInstanceOf[java.util.Map[String, AnyRef]]
    if (isValidEvent(eData.get("type").asInstanceOf[String])) {
      val courseId = eData.get("courseId").asInstanceOf[String]
      val batchId = eData.get("batchId").asInstanceOf[String]
      val userId = eData.get("userId").asInstanceOf[String]
      print("Enrolment Event" + event)

    } else {
      println("Invalid Event")
    }

  }

  def isValidEvent(eType: String): Boolean = {
    StringUtils.equalsIgnoreCase(eType, config.supportedEventType)
  }

  def getUserAggQuery(progress: UserActivityAgg):
  Update.Where = {
    QueryBuilder.update(config.dbKeyspace, config.dbUserActivityAggTable)
      .`with`(QueryBuilder.putAll("agg", progress.agg.asJava))
      .and(QueryBuilder.putAll("agg_last_updated", progress.agg_last_updated.asJava))
      .where(QueryBuilder.eq("activity_id", progress.activity_id))
      .and(QueryBuilder.eq("activity_type", progress.activity_type))
      .and(QueryBuilder.eq("context_id", progress.context_id))
      .and(QueryBuilder.eq("user_id", progress.user_id))
  }

  def getEnrolmentUpdateQuery(enrolment: CollectionProgress): Update.Where = {
    logger.info("Enrolment updated for userId: " + enrolment.userId + " batchId: " + enrolment.batchId)
    QueryBuilder.update(config.dbKeyspace, config.dbUserEnrolmentsTable)
      .`with`(QueryBuilder.set("status", 1))
      .and(QueryBuilder.set("progress", enrolment.progress))
      .and(QueryBuilder.set("contentstatus", enrolment.contentStatus.asJava))
      .and(QueryBuilder.set("datetime", System.currentTimeMillis))
      .where(QueryBuilder.eq("userid", enrolment.userId))
      .and(QueryBuilder.eq("courseid", enrolment.courseId))
      .and(QueryBuilder.eq("batchid", enrolment.batchId))
  }

}


