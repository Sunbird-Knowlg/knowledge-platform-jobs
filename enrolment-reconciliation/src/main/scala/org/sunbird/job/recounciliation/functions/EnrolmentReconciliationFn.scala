package org.sunbird.job.recounciliation.functions

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.twitter.storehaus.cache.TTLCache
import com.twitter.util.Duration
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.recounciliation.domain._
import org.sunbird.job.recounciliation.task.EnrolmentReconciliationConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._


class EnrolmentReconciliationFn(config: EnrolmentReconciliationConfig,  httpUtil: HttpUtil, @transient var cassandraUtil: CassandraUtil = null)
                               (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Event, String](config) {

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
    List(config.totalEventCount,
      config.failedEventCount,
      config.dbUpdateCount,
      config.dbReadCount,
      config.cacheHitCount,
      config.cacheMissCount,
      config.retiredCCEventsCount,
      config.skipEventsCount
    )
  }


  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventCount)
    if (event.isValidEvent(config.supportedEventType)) {
      // Fetch the content status from the table in batch format
      val dbUserConsumption: List[UserContentConsumption] = getContentStatusFromDB(
        Map("courseId" -> event.courseId, "userId" -> event.userId, "batchId" -> event.batchId), metrics)

      val courseAggregations = dbUserConsumption.flatMap{ userConsumption =>
        // Course Level Agg using the merged data of ContentConsumption per user, course and batch.
        val optCourseAgg = courseActivityAgg(userConsumption, context)(metrics)
        val courseAggs = if (optCourseAgg.nonEmpty) List(optCourseAgg.get) else List()

        // Identify the children of the course (only collections) for which aggregates computation required.
        // Computation of aggregates using leafNodes (of the specific collection) and user completed contents.
        // Here computing only "completedCount" aggregate.
        if (config.moduleAggEnabled) {
          val courseChildrenAggs = courseChildrenActivityAgg(userConsumption)(metrics)
          courseAggs ++ courseChildrenAggs
        } else courseAggs
      }
      // Saving all queries for course and it's children (only collection) aggregates.
      val aggQueries = courseAggregations.map(agg => getUserAggQuery(agg.activityAgg))
      updateDB(config.thresholdBatchWriteSize, aggQueries)(metrics)

      // Saving enrolment completion data.
      val collectionProgressList = courseAggregations.filter(agg => agg.collectionProgress.nonEmpty).map(agg => agg.collectionProgress.get)
      val collectionProgressUpdateList = collectionProgressList.filter(progress => !progress.completed)
      context.output(config.collectionUpdateOutputTag, collectionProgressUpdateList)
      
      val collectionProgressCompleteList = collectionProgressList.filter(progress => progress.completed)
      context.output(config.collectionCompleteOutputTag, collectionProgressCompleteList)

    } else {
      metrics.incCounter(config.skipEventsCount)
      logger.info("Event skipped as it is invalid ", event)
    }

  }

  def getUserAggQuery(progress: UserActivityAgg):
  Update.Where = {
    QueryBuilder.update(config.dbKeyspace, config.dbUserActivityAggTable)
      .`with`(QueryBuilder.putAll("aggregates", progress.aggregates.asJava))
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

  def getContentStatusFromDB(eDataBatch: Map[String, String], metrics: Metrics): List[UserContentConsumption] = {
    val primaryFields = Map(
      config.userId.toLowerCase() -> eDataBatch.getOrElse("userId", ""),
      config.batchId.toLowerCase -> eDataBatch.getOrElse("batchId", ""),
      config.courseId.toLowerCase -> eDataBatch.getOrElse("courseId", "")
    )

    val records = Option(readFromDB(primaryFields, config.dbKeyspace, config.dbUserContentConsumptionTable, metrics))
    val contentConsumption = records.map(record => record.groupBy(col => Map(config.batchId -> col.getObject(config.batchId.toLowerCase()).asInstanceOf[String], config.userId -> col.getObject(config.userId.toLowerCase()).asInstanceOf[String], config.courseId -> col.getObject(config.courseId.toLowerCase()).asInstanceOf[String])))
      .map(groupedRecords => groupedRecords.map(entry => {
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

        UserContentConsumption(userId, batchId, courseId, consumptionList)
      })).getOrElse(List[UserContentConsumption]()).toList
    contentConsumption
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

  /**
   * Course Level Agg using the merged data of ContentConsumption per user, course and batch.
   */
  def courseActivityAgg(userConsumption: UserContentConsumption, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics): Option[UserEnrolmentAgg] = {
    val courseId = userConsumption.courseId
    val userId = userConsumption.userId
    val contextId = "cb:" + userConsumption.batchId
    val key = s"$courseId:$courseId:${config.leafNodes}"
    val leafNodes = readFromCache(key, metrics).distinct
    if (leafNodes.isEmpty) {
      logger.error(s"leaf nodes are not available for: $key")
      context.output(config.failedEventOutputTag, gson.toJson(userConsumption))
      val status = getCollectionStatus(courseId)
      if (StringUtils.equals("Retired", status)) {
        metrics.incCounter(config.retiredCCEventsCount)
        println(s"contents consumed from a retired collection: $courseId")
        logger.warn(s"contents consumed from a retired collection: $courseId")
        None
      } else {
        metrics.incCounter(config.failedEventCount)
        val message = s"leaf nodes are not available for a published collection: $courseId"
        logger.error(message)
        throw new Exception(message)
      }
    } else {
      val completedCount = leafNodes.intersect(userConsumption.contents.filter(cc => cc._2.status == 2).map(cc => cc._2.contentId).toList.distinct).size
      val contentStatus = userConsumption.contents.map(cc => (cc._2.contentId, cc._2.status)).toMap
      val inputContents = userConsumption.contents.filter(cc => cc._2.fromInput).keys.toList
      val collectionProgress = if (completedCount >= leafNodes.size) {
        Option(CollectionProgress(userId, userConsumption.batchId, courseId, completedCount, new java.util.Date(), contentStatus, inputContents, true))
      } else {
        Option(CollectionProgress(userId, userConsumption.batchId, courseId, completedCount, null, contentStatus, inputContents))
      }
      Option(UserEnrolmentAgg(UserActivityAgg("Course", userId, courseId, contextId, Map("completedCount" -> completedCount.toDouble), Map("completedCount" -> System.currentTimeMillis())), collectionProgress))
    }
  }

  def readFromCache(key: String, metrics: Metrics): List[String] = {
    metrics.incCounter(config.cacheHitCount)
    val list = cache.getKeyMembers(key)
    if (CollectionUtils.isEmpty(list)) {
      metrics.incCounter(config.cacheMissCount)
      logger.info("Redis cache (smembers) not available for key: " + key)
    }
    list.asScala.toList
  }

  /**
   * Identified the children of the course (only collections) for which aggregates computation required.
   * Computation of aggregates using leafNodes (of the specific collection) and user completed contents.
   * Here computing only "completedCount" aggregate.
   */
  def courseChildrenActivityAgg(userConsumption: UserContentConsumption)(implicit metrics: Metrics): List[UserEnrolmentAgg] = {
    val courseId = userConsumption.courseId
    val userId = userConsumption.userId
    val contextId = "cb:" + userConsumption.batchId

    // These are the child collections which require computation of aggregates - for this user.
    val ancestors = userConsumption.contents.mapValues(content => {
      val contentId = content.contentId
      readFromCache(key = s"$courseId:$contentId:${config.ancestors}", metrics)
    }).values.flatten.filter(a => !StringUtils.equals(a, courseId)).toList.distinct

    // LeafNodes of the identified child collections - for this user.
    val collectionsWithLeafNodes = ancestors.map(unitId => {
      (unitId, readFromCache(key = s"$courseId:$unitId:${config.leafNodes}", metrics).distinct)
    }).toMap

    // Content completed - By this user.
    val userCompletedContents = userConsumption.contents.filter(cc => cc._2.status == 2).map(cc => cc._2.contentId).toList.distinct

    // Child Collection UserAggregate list - for this user.
    collectionsWithLeafNodes.map(e => {
      val collectionId = e._1
      val leafNodes = e._2
      val completedCount = leafNodes.intersect(userCompletedContents).size
      /* TODO - List
       TODO 1. Generalise activityType from "Course" to "Collection".
       TODO 2.Identify how to generate start and end event for CourseUnit.
       */
      val activityAgg = UserActivityAgg("Course", userId, collectionId, contextId, Map("completedCount" -> completedCount), Map("completedCount" -> System.currentTimeMillis()))
      UserEnrolmentAgg(activityAgg, None)
    }).toList
  }

  /**
   * Method to update the specific table in a batch format.
   */
  def updateDB(batchSize: Int, queriesList: List[Update.Where])(implicit metrics: Metrics): Unit = {
    val groupedQueries = queriesList.grouped(batchSize).toList
    groupedQueries.foreach(queries => {
      val cqlBatch = QueryBuilder.batch()
      queries.map(query => cqlBatch.add(query))
      val result = cassandraUtil.upsert(cqlBatch.toString)
      if (result) {
        metrics.incCounter(config.dbUpdateCount)
      } else {
        val msg = "Database update has failed: " + cqlBatch.toString
        logger.error(msg)
        throw new Exception(msg)
      }
    })
  }

}


