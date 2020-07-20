package org.sunbird.async.functions

import java.lang.reflect.Type
import java.{lang, util}

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.slf4j.LoggerFactory
import org.sunbird.async.core.cache.{DataCache, RedisConnect}
import org.sunbird.async.core.job.{Metrics, WindowBaseProcessFunction}
import org.sunbird.async.core.util.CassandraUtil
import org.sunbird.async.domain._
import org.sunbird.async.task.CourseAggregateUpdaterConfig

import scala.collection.JavaConverters._


class CourseAggregatesFunction(config: CourseAggregateUpdaterConfig)(implicit val stringTypeInfo: TypeInformation[String], @transient var cassandraUtil: CassandraUtil = null) extends WindowBaseProcessFunction[util.Map[String, AnyRef], String, String](config) {
  val mapType: Type = new TypeToken[util.Map[String, AnyRef]]() {}.getType
  private[this] val logger = LoggerFactory.getLogger(classOf[CourseAggregatesFunction])
  private var cache: DataCache = _
  lazy private val gson = new Gson()

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.batchEnrolmentUpdateEventCount, config.dbUpdateCount, config.dbReadCount, config.cacheHitCount, config.skipEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    cache = new DataCache(config, new RedisConnect(config), config.nodeStore, List())
    cache.init()
  }

  override def close(): Unit = {
    cassandraUtil.close()
    cache.close()
    super.close()
  }

  def process(key: String, context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context, events: lang.Iterable[util.Map[String, AnyRef]], metrics: Metrics): Unit = {
    logger.info("EventsSize" + events.asScala.toList.size)
    val contentConsumptionEvents = events.asScala.filter(event => {
      val isBatchEnrollmentEvent: Boolean = StringUtils.equalsIgnoreCase(event.get(config.eData).asInstanceOf[util.Map[String, AnyRef]].asScala.getOrElse(config.action, "").asInstanceOf[String], config.batchEnrolmentUpdateCode)
      if (isBatchEnrollmentEvent) {
        logger.info("Processing batch-enrolment-update - MID: " + event.get("mid"))
        metrics.incCounter(config.batchEnrolmentUpdateEventCount)
        isBatchEnrollmentEvent
      } else {
        metrics.incCounter(config.skipEventsCount)
        isBatchEnrollmentEvent
      }
    })
    val eDataBatch: List[Map[String, AnyRef]] = contentConsumptionEvents.map(f => f.get(config.eData).asInstanceOf[util.Map[String, AnyRef]].asScala.toMap).toList
    // Grouping the contents to avoid the duplicate of contents
    val groupedData: List[Map[String, AnyRef]] = eDataBatch.groupBy(key => (key.get(config.courseId), key.get(config.batchId), key.get(config.userId)))
      .values.map(value => Map(config.batchId -> value.head(config.batchId), config.userId -> value.head(config.userId), config.courseId -> value.head(config.courseId),
      config.contents -> value.flatMap(contents => contents(config.contents).asInstanceOf[util.List[Map[String, AnyRef]]].asScala.toList))).toList

    // content status from the telemetry event.
    val inputUserConsumptionList: List[UserContentConsumption] = groupedData.map(ed => {
      val userConsumedContents = ed(config.contents).asInstanceOf[List[Map[String, AnyRef]]]
      val enrichedContents = getContentStatusFromEvent(userConsumedContents)
      UserContentConsumption(ed(config.userId).asInstanceOf[String], ed(config.batchId).asInstanceOf[String], ed(config.courseId).asInstanceOf[String], enrichedContents)
    })

    // Fetch the content status from the table in batch format
    val dbUserConsumption: Map[String, UserContentConsumption] = getContentStatusFromDB(eDataBatch, metrics).map(e => (getUCKey(e), e)).toMap

    // Final User's ContentConsumption after merging with DB data.
    // Here we have final viewcount, completedcount and identified the content which should generate AUDIT events for start and complete.
    val finalUserConsumptionList = inputUserConsumptionList.map(inputData => {
      val dbData = dbUserConsumption.getOrElse(getUCKey(inputData), UserContentConsumption(inputData.userId, inputData.batchId, inputData.courseId, Map()))
      finalUserConsumption(inputData, dbData)(metrics)
    })

    // user_content_consumption update with viewcount and completedcout.
    val userConsumptionQueries = finalUserConsumptionList.map(userConsumption => getContentConsumptionQueries(userConsumption)).flatten.toList
    updateDB(config.maxQueryWriteBatchSize, userConsumptionQueries)(metrics)

    // Course Level Agg using the merged data of ContentConsumption per user, course and batch.
    val courseAggs = finalUserConsumptionList.map(userConsumption => courseActivityAgg(userConsumption)(metrics))

    // Identified the children of the course (only collections) for which aggregates computation required.
    // Computation of aggregates using leafNodes (of the specific collection) and user completed contents.
    // Here computing only "completedCount" aggregate.
    val courseChildrenAggs = finalUserConsumptionList.map(userConsumption => courseChildrenActivityAgg(userConsumption)(metrics)).flatten

    // Saving all queries for course and it's children (only collection) aggregates.
    val aggQueries = (courseAggs ++ courseChildrenAggs).map(agg => getUserAggQuery(agg))
    updateDB(config.maxQueryWriteBatchSize, aggQueries)(metrics)

    // Content AUDIT Event generation and pushing to output tag.
    finalUserConsumptionList.map(userConsumption => contentAuditEvents(userConsumption))
      .flatten.foreach(event => context.output(config.auditEventOutputTag, gson.toJson(event)))

  }

  /**
   * Course Level Agg using the merged data of ContentConsumption per user, course and batch.
   *
   * @param userConsumption
   * @param metrics
   * @return
   */
  def courseActivityAgg(userConsumption: UserContentConsumption)(implicit metrics: Metrics): UserActivityAgg = {
    val courseId = userConsumption.courseId
    val userId = userConsumption.userId
    val contextId = "cb:" + userConsumption.batchId
    val leafNodes = readFromCache(key = s"$courseId:$courseId:${config.leafNodes}", metrics).distinct
    val completedCount = leafNodes.intersect(userConsumption.contents.filter(cc => cc._2.status == 2).map(cc => cc._2.contentId).toList.distinct).size
    UserActivityAgg("course", userId, courseId, contextId, Map("completedCount" -> completedCount), Map("completedCount" -> System.currentTimeMillis()))
  }

  /**
   * Identified the children of the course (only collections) for which aggregates computation required.
   * Computation of aggregates using leafNodes (of the specific collection) and user completed contents.
   * Here computing only "completedCount" aggregate.
   *
   * @param userConsumption
   * @param metrics
   * @return
   */
  def courseChildrenActivityAgg(userConsumption: UserContentConsumption)(implicit metrics: Metrics): List[UserActivityAgg] = {
    val courseId = userConsumption.courseId
    val userId = userConsumption.userId
    val contextId = "cb:" + userConsumption.batchId

    // These are the child collections which require computation of aggregates - for this user.
    val ancestors = userConsumption.contents.mapValues(content => {
      val contentId = content.contentId
      readFromCache(key = s"$courseId:$contentId:${config.ancestors}", metrics)
    }).values.flatten.filter(_ > courseId).toList.distinct

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
      // TODO - Identify how to generate start and end event for CourseUnit.
      UserActivityAgg("course", userId, collectionId, contextId, Map("completedCount" -> completedCount), Map("completedCount" -> System.currentTimeMillis()))
    }).toList
  }

  /**
   * Generation of a "String" key for UserContentConsumption.
   *
   * @param userConsumption
   * @return
   */
  def getUCKey(userConsumption: UserContentConsumption): String = {
    userConsumption.userId + ":" + userConsumption.courseId + ":" + userConsumption.batchId
  }

  /**
   * Merging the Input and DB ContentStatus data of a User, Course and Batch (Enrolment)
   * This is the critical part of the code.
   *
   * @param inputData
   * @param dbData
   * @param metrics
   * @return
   */
  def finalUserConsumption(inputData: UserContentConsumption, dbData: UserContentConsumption)(implicit metrics: Metrics): UserContentConsumption = {
    val dbContents = dbData.contents
    val processedContents = inputData.contents.map(ccMap => {
      val contentId = ccMap._1
      // ContentStatus from Input (From events of Input topic).
      val inputCC = ccMap._2
      // ContentStatus from DB.
      val dbCC: ContentStatus = dbContents.getOrElse(contentId, ContentStatus(contentId, 0, 0, 0))
      val finalStatus = List(inputCC.status, dbCC.status).max // Final status is max of DB and Input ContentStatus.
      val views = sumFunc(List(inputCC, dbCC), (x: ContentStatus) => {
        x.viewCount
      }) // View Count is sum of DB and Input ContentStatus.
      val completion = sumFunc(List(inputCC, dbCC), (x: ContentStatus) => {
        x.completedCount
      }) // Completed Count is sum of DB and Input ContentStatus.
      val eventsFor: List[String] = getEventActions(dbCC, inputCC)
      // Merged ContentStatus.
      (contentId, ContentStatus(contentId, finalStatus, completion, views, eventsFor))
    }).toMap

    val existing = processedContents.keys.toList
    val remainingContents = dbData.contents.filterKeys(key => !existing.contains(key))
    val finalContentsMap = processedContents ++ remainingContents
    UserContentConsumption(inputData.userId, inputData.batchId, inputData.courseId, finalContentsMap)
  }

  /**
   * This will identify whether this is the start or complete of the Content by User.
   *
   * @param dbCC
   * @param inputCC
   * @return List - Actions - "start" and "complete".
   */
  def getEventActions(dbCC: ContentStatus, inputCC: ContentStatus): List[String] = {
    val startAction = if (dbCC.viewCount == 0) List("start") else List()
    val completeAction = if (dbCC.completedCount == 0 && inputCC.completedCount > 0) List("complete") else List()
    startAction ::: completeAction
  }

  /**
   * Generic method to read data from DB (Cassandra).
   *
   * @param columns
   * @param keySpace
   * @param table
   * @param metrics
   * @return
   */
  def readFromDB(columns: Map[String, AnyRef], keySpace: String, table: String, metrics: Metrics): List[Row]
  = {
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
   // logger.info("DB Read" + selectWhere.toString)
    metrics.incCounter(config.dbReadCount)
    cassandraUtil.find(selectWhere.toString).asScala.toList

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
       // logger.info("DataBase Update got success" + cqlBatch.toString)
        metrics.incCounter(config.successEventCount)
        metrics.incCounter(config.dbUpdateCount)
      } else {
       // logger.info("Database update has failed" + cqlBatch.toString)
      }
    })
  }

  def readFromCache(key: String, metrics: Metrics): List[String] = {
    //logger.info("redis key" + key)
    //logger.info("redis db" + cache.dbIndex)
    metrics.incCounter(config.cacheHitCount)
    val list = cache.getKeyMembers(key)
    //if (CollectionUtils.isEmpty(list))
      //logger.info("Redis cache (smembers) not available for key: " + key)
    list.asScala.toList
  }

  def getUserAggQuery(progress: UserActivityAgg):
  Update.Where = {
    QueryBuilder.update(config.dbKeyspace, config.dbUserActivityAggTable)
      .`with`(QueryBuilder.putAll(config.agg, progress.agg.asJava))
      .and(QueryBuilder.putAll(config.aggLastUpdated, progress.agg_last_updated.asJava))
      .where(QueryBuilder.eq(config.activityId, progress.activity_id))
      .and(QueryBuilder.eq(config.activityType, progress.activity_type))
      .and(QueryBuilder.eq(config.contextId, progress.context_id))
      .and(QueryBuilder.eq(config.activityUser, progress.user_id))
  }

  /**
   * Creates the cql query for content consumption table
   */
  def getContentConsumptionQueries(userContentConsumption: UserContentConsumption): List[Update.Where] = {
    userContentConsumption.contents.mapValues(content => {
      QueryBuilder.update(config.dbKeyspace, config.dbUserContentConsumptionTable)
        .`with`(QueryBuilder.set(config.viewcount, content.viewCount))
        .and(QueryBuilder.set(config.completedcount, content.completedCount))
        .where(QueryBuilder.eq(config.batchId.toLowerCase(), userContentConsumption.batchId))
        .and(QueryBuilder.eq(config.courseId.toLowerCase(), userContentConsumption.courseId))
        .and(QueryBuilder.eq(config.userId.toLowerCase(), userContentConsumption.userId))
        .and(QueryBuilder.eq(config.contentId.toLowerCase(), content.contentId))
    }).values.toList
  }

  /**
   * Method to get the content status object in map format ex: (do_5874308329084 -> 2, do_59485345435 -> 3)
   * It always takes the highest precedence progress values for the contents ex: (do_5874308329084 -> 2, do_5874308329084 -> 1, do_59485345435 -> 3) => (do_5874308329084 -> 2, do_59485345435 -> 3)
   *
   * Ex: Map("C1"->2, "C2" ->1)
   *
   */
  def getContentStatusFromEvent(contents: List[Map[String, AnyRef]]): Map[String, ContentStatus] = {
    val enrichedContents = contents.asInstanceOf[List[util.Map[String, AnyRef]]].map(content => {
      val map = content.asScala.toMap
      (map.getOrElse(config.contentId, "").asInstanceOf[String], map.getOrElse(config.status, 0).asInstanceOf[Number])
    }).filter(t => StringUtils.isNotBlank(t._1) && (t._2.intValue() > 0))
      .map(x => {
        val completedCount = if (x._2.intValue() == 2) 1 else 0
        ContentStatus(x._1, x._2.intValue(), completedCount)
      }).groupBy(f => f.contentId)

    enrichedContents.map(content => {
      val consumedList = content._2
      val finalStatus = consumedList.map(x => x.status).max
      val views = sumFunc(consumedList, (x: ContentStatus) => {
        x.viewCount
      })
      val completion = sumFunc(consumedList, (x: ContentStatus) => {
        x.completedCount
      })
      (content._1, ContentStatus(content._1, finalStatus, completion, views))
    }).toMap
  }

  /**
   * Computation of Sum for viewCount and completedCount.
   *
   * @param list
   * @param valFunc
   * @return
   */
  private def sumFunc(list: List[ContentStatus], valFunc: (ContentStatus) => Int): Int = list.map(x => valFunc(x)).sum


  /**
   * Method to get the content status from the database
   *
   * Ex: List(Map("courseId" -> "do_43795", batchId -> "batch1", userId->"user001", contentStatus -> Map("C1"->2, "C2" ->1)))
   *
   */
  def getContentStatusFromDB(eDataBatch: List[Map[String, AnyRef]], metrics: Metrics): List[UserContentConsumption] = {
    val primaryFields = Map(config.userId.toLowerCase() -> eDataBatch.map(x => x(config.userId)).toList.distinct, config.batchId.toLowerCase -> eDataBatch.map(x => x(config.batchId)).toList.distinct, config.courseId.toLowerCase -> eDataBatch.map(x => x(config.courseId)).toList.distinct)
    val records = Option(readFromDB(primaryFields, config.dbKeyspace, config.dbUserContentConsumptionTable, metrics))

    records.map(record => record.groupBy(col => Map(config.batchId -> col.getObject(config.batchId.toLowerCase()).asInstanceOf[String], config.userId -> col.getObject(config.userId.toLowerCase()).asInstanceOf[String], config.courseId -> col.getObject(config.courseId.toLowerCase()).asInstanceOf[String])))
      .map(groupedRecords => groupedRecords.map(entry => {
        val identifierMap = entry._1
        val consumptionList = entry._2.flatMap(row => Map(row.getObject(config.contentId.toLowerCase()).asInstanceOf[String] -> Map(config.status -> row.getObject(config.status), config.viewcount -> row.getObject(config.viewcount), config.completedcount -> row.getObject(config.completedcount))))
          .map(entry => {
            val contentStatus = entry._2.filter(x => x._2 != null)
            val contentId = entry._1
            val status = contentStatus.getOrElse(config.status, 1).asInstanceOf[Number].intValue()
            val viewCount = contentStatus.get(config.viewcount).getOrElse(0).asInstanceOf[Number].intValue()
            val defaultCompletedCount = if (status == 2) 1 else 0
            val completedCount = contentStatus.getOrElse(config.completedcount, defaultCompletedCount).asInstanceOf[Number].intValue()
            (contentId, ContentStatus(contentId, status, completedCount, viewCount))
          }).toMap

        val userId = identifierMap.get(config.userId).get
        val batchId = identifierMap.get(config.batchId).get
        val courseId = identifierMap.get(config.courseId).get
        UserContentConsumption(userId, batchId, courseId, consumptionList)

      })).getOrElse(List()).toList
  }

  /**
   * Content - AUDIT Event Generation using UserContentConsumption
   * "eventsFor" - will have the action (or type) for the event to generate.
   *
   * @param userConsumption
   * @return
   */
  def contentAuditEvents(userConsumption: UserContentConsumption): List[TelemetryEvent] = {
    val userId = userConsumption.userId
    val courseId = userConsumption.courseId
    val batchId = userConsumption.batchId
    val contentsForEvents = userConsumption.contents.filter(c => c._2.eventsFor.nonEmpty).values
    contentsForEvents.map(c => {
      c.eventsFor.map(action => {
        val properties = if (StringUtils.equalsIgnoreCase(action, "complete")) Array("viewcount", "completedcount") else Array("viewcount")
        TelemetryEvent(
          actor = ActorObject(id = userId),
          edata = EventData(props = properties, `type` = action), // action values are "start", "complete".
          context = EventContext(cdata = Array(Map("type" -> "CourseBatch", "id" -> batchId).asJava)),
          `object` = EventObject(id = c.contentId, `type` = "Content", rollup = Map[String, String]("l1" -> courseId).asJava)
        )
      })
    }).flatten.toList
  }
}

