package org.sunbird.job.functions

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
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
import org.sunbird.job.domain.{CollectionProgress, ContentStatus, UserActivityAgg, UserContentConsumption}
import org.sunbird.job.task.EnrolmentReconciliationConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._


class EnrolmentReconciliationFn(config: EnrolmentReconciliationConfig,  httpUtil: HttpUtil, @transient var cassandraUtil: CassandraUtil = null)
                               (implicit val stringTypeInfo: TypeInformation[String])
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
    List(config.failedEventCount,
      config.dbUpdateCount,
      config.dbReadCount,
      config.cacheHitCount,
      config.cacheMissCount,
      config.retiredCCEventsCount
    )
  }


  override def processElement(event: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    val eData = event.asScala.getOrElse("edata", Map()).asInstanceOf[java.util.Map[String, AnyRef]].asScala
    if (isValidEvent(eData.getOrElse("action", "").asInstanceOf[String])) {
      val courseId = eData.getOrElse("courseId", "").asInstanceOf[String]
      val batchId = eData.getOrElse("batchId", "").asInstanceOf[String]
      val userId = eData.getOrElse("userId", "").asInstanceOf[String]
      println("It's Valid ")

      // Fetch the content status from the table in batch format
      val dbUserConsumption: Map[String, UserContentConsumption] = getContentStatusFromDB(
        Map("courseId" -> courseId, "userId" -> userId, "batchId" -> batchId),
        metrics)

    } else {
      println("It's InValid ")

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

  def getCollectionStatus(collectionId: String): String = {
    val cacheStatus = collectionStatusCache.getNonExpired(collectionId).getOrElse("")
    if (StringUtils.isEmpty(cacheStatus)) {
      val dbStatus = getDBStatus(collectionId)
      collectionStatusCache.putClocked(collectionId, dbStatus)
      dbStatus
    } else cacheStatus
  }

  def getDBStatus(collectionId: String): String = {
    val requestBody =
      s"""{
         |    "request": {
         |        "filters": {
         |            "objectType": "Collection",
         |            "identifier": "$collectionId",
         |            "status": ["Live", "Unlisted", "Retired"]
         |        },
         |        "fields": ["status"]
         |    }
         |}""".stripMargin

    val response = httpUtil.post(config.searchAPIURL, requestBody)
    if (response.status == 200) {
      val responseBody = gson.fromJson(response.body, classOf[java.util.Map[String, AnyRef]])
      val result = responseBody.getOrDefault("result", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
      val count = result.getOrDefault("count", 0.asInstanceOf[Number]).asInstanceOf[Number].intValue()
      if (count > 0) {
        val list = result.getOrDefault("content", new java.util.ArrayList[java.util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        list.asScala.head.get("status").asInstanceOf[String]
      } else throw new Exception(s"There are no published or retired collection with id: $collectionId")
    } else {
      logger.error("search-service error: " + response.body)
      throw new Exception("search-service not returning error:" + response.status)
    }
  }

  def getContentStatusFromDB(eDataBatch: Map[String, String], metrics: Metrics): Map[String, UserContentConsumption] = {

    val contentConsumption = scala.collection.mutable.Map[String, UserContentConsumption]()
    val primaryFields = Map(
      config.userId.toLowerCase() -> eDataBatch.getOrElse("userId", ""),
      config.batchId.toLowerCase -> eDataBatch.getOrElse("userId", ""),
      config.courseId.toLowerCase -> eDataBatch.getOrElse("userId", "")
    )

    val records = Option(readFromDB(primaryFields, config.dbKeyspace, config.dbUserContentConsumptionTable, metrics))
    records.map(record => record.groupBy(col => Map(config.batchId -> col.getObject(config.batchId.toLowerCase()).asInstanceOf[String], config.userId -> col.getObject(config.userId.toLowerCase()).asInstanceOf[String], config.courseId -> col.getObject(config.courseId.toLowerCase()).asInstanceOf[String])))
      .foreach(groupedRecords => groupedRecords.map(entry => {
        val identifierMap = entry._1
        val consumptionList = entry._2.flatMap(row => Map(row.getObject(config.contentId.toLowerCase()).asInstanceOf[String] -> Map(config.status -> row.getObject(config.status), config.viewcount -> row.getObject(config.viewcount), config.completedcount -> row.getObject(config.completedcount))))
          .map(entry => {
            val contentStatus = entry._2.filter(x => x._2 != null)
            val contentId = entry._1
            val status = contentStatus.getOrElse(config.status, 1).asInstanceOf[Number].intValue()
            val viewCount = contentStatus.getOrElse(config.viewcount, 0).asInstanceOf[Number].intValue()
            val completedCount = contentStatus.getOrElse(config.completedcount, 0).asInstanceOf[Number].intValue()
            (contentId, ContentStatus(contentId, status, completedCount, viewCount, false))
          }).toMap

        val userId = identifierMap(config.userId)
        val batchId = identifierMap(config.batchId)
        val courseId = identifierMap(config.courseId)

        val userContentConsumption = UserContentConsumption(userId, batchId, courseId, consumptionList)
        contentConsumption += getUCKey(userContentConsumption) -> userContentConsumption

      }))
    contentConsumption.toMap
  }

  def readFromDB(columns: Map[String, AnyRef], keySpace: String, table: String, metrics: Metrics): List[Row] = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(keySpace, table).
      where()
    columns.map(col => {
      col._2 match {
        case value: List[Any] =>
          selectWhere.and(QueryBuilder.in(col._1, value.asJava))
        case _ =>
          selectWhere.and(QueryBuilder.eq(col._1, col._2))
      }
    })
    metrics.incCounter(config.dbReadCount)
    cassandraUtil.find(selectWhere.toString).asScala.toList

  }

  def getUCKey(userConsumption: UserContentConsumption): String = {
    userConsumption.userId + ":" + userConsumption.courseId + ":" + userConsumption.batchId
  }


}


