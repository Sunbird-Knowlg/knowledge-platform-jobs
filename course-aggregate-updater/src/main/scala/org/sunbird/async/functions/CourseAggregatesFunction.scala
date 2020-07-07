package org.sunbird.async.functions

import java.lang.reflect.Type
import java.{lang, util}

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import org.sunbird.async.core.cache.{DataCache, RedisConnect}
import org.sunbird.async.core.job.{Metrics, WindowBaseProcessFunction}
import org.sunbird.async.core.util.CassandraUtil
import org.sunbird.async.domain._
import org.sunbird.async.task.CourseAggregateUpdaterConfig

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class CourseAggregatesFunction(config: CourseAggregateUpdaterConfig)(implicit val stringTypeInfo: TypeInformation[String], @transient var cassandraUtil: CassandraUtil = null) extends WindowBaseProcessFunction[util.Map[String, AnyRef], String, String](config) {
  val mapType: Type = new TypeToken[util.Map[String, AnyRef]]() {}.getType
  private[this] val logger = LoggerFactory.getLogger(classOf[CourseAggregatesFunction])
  private var cache: DataCache = _
  lazy private val gson = new Gson()
  var progressBatch = new ListBuffer[Update.Where]()
  var contentConsumptionBatch = new ListBuffer[Update.Where]()

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.batchEnrolmentUpdateEventCount, config.dbUpdateCount, config.dbReadCount, config.cacheHitCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    cache = new DataCache(config, new RedisConnect(config), config.nodeStore, List())
    cache.init()
  }

  override def close(): Unit = {
    super.close()
  }

  def process(key: String, context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context, events: lang.Iterable[util.Map[String, AnyRef]], metrics: Metrics): Unit = {
    val batchEnrolmentEvents = events.asScala.filter(event => StringUtils.equalsIgnoreCase(event.get(config.eData).asInstanceOf[util.Map[String, AnyRef]].asScala(config.action).asInstanceOf[String], config.batchEnrolmentUpdateCode))
    val eDataBatch: List[Map[String, AnyRef]] = batchEnrolmentEvents.map(f => {
      metrics.incCounter(config.batchEnrolmentUpdateEventCount)
      f.get(config.eData).asInstanceOf[util.Map[String, AnyRef]].asScala.toMap
    }).toList

    // Grouping the contents to avoid the duplicate of contents
    val groupedData: List[Map[String, AnyRef]] = eDataBatch.groupBy(key => (key.get(config.courseId), key.get(config.batchId), key.get(config.userId)))
      .values.map(value => Map(config.batchId -> value.head(config.batchId), config.userId -> value.head(config.userId), config.courseId -> value.head(config.courseId),
      config.contents -> value.flatMap(contents => contents(config.contents).asInstanceOf[util.List[Map[String, AnyRef]]].asScala.toList))).toList

    // content status from the telemetry event.
    val csFromEvents: List[Map[String, AnyRef]] = groupedData.map(ed => ed ++ Map(config.contentStatus -> getContentStatusFromEvent(ed(config.contents).asInstanceOf[List[Map[String, AnyRef]]])))

    // Fetch the content status from the table in batch format
    val csFromDb: List[Map[String, AnyRef]] = getContentStatusFromDB(eDataBatch, metrics)

    // CourseProgress
    csFromEvents.map(csFromEvent => getCourseProgress(csFromEvent, csFromDb, metrics)
      .map(course => {
        progressBatch += getUserAggQuery(course._2.asInstanceOf[Progress], config.dbKeyspace, config.dbUserActivityAggTable)
        getContentConsumptionQuery(course._2.asInstanceOf[Progress], config.dbKeyspace, config.dbContentConsumptionTable).map(query => contentConsumptionBatch += query) // create a content consumption query
        course._2.contentStats.map(stats => {
          if (stats.getOrElse(config.viewcount, 0).asInstanceOf[Int] == 0) context.output(config.auditEventOutputTag, gson.toJson(generateTelemetry(csFromEvent, config.contents, isCompleted = false, course._2.activity_id))) // Generate start event
          if (stats.getOrElse(config.completedcount, 0).asInstanceOf[Int] == 1) context.output(config.auditEventOutputTag, gson.toJson(generateTelemetry(csFromEvent, config.contents, isCompleted = true, course._2.activity_id))) // Generate end event
        })
      }))

    // Unit Progress
    csFromEvents.map(csFromEvent => getUnitProgress(csFromEvent, csFromDb, context, metrics)
      .map(unit => progressBatch += getUserAggQuery(unit._2, config.dbKeyspace, config.dbUserActivityAggTable))) // create user_agg query

    // Update batch of queries into cassandra
    updateDB(config.maxQueryWriteBatchSize, progressBatch, metrics)
    updateDB(config.maxQueryWriteBatchSize, contentConsumptionBatch, metrics)

  }


  /**
   * Method to compute the progress for course level
   */
  def getCourseProgress(csFromEvent: Map[String, AnyRef],
                        csFromDb: List[Map[String, AnyRef]],
                        metrics: Metrics): Map[String, Progress] = {
    val courseId = csFromEvent.getOrElse(config.courseId, null).asInstanceOf[String]
    val batchId = csFromEvent.getOrElse(config.batchId, null).asInstanceOf[String]
    val userId = csFromEvent.getOrElse(config.userId, null).asInstanceOf[String]
    val leafNodes = readFromCache(key = s"$courseId:$courseId:${config.leafNodes}", metrics)
    if (leafNodes.isEmpty) {
      metrics.incCounter(config.failedEventCount)
      throw new Exception(s"LeafNodes are not available. courseId:$courseId")
    }
    val courseContentsStatusFromDb = csFromDb.filter(cs => cs(config.courseId).asInstanceOf[String] == courseId && cs(config.batchId).asInstanceOf[String] == batchId && cs(config.userId).asInstanceOf[String] == userId)
    Map(courseId -> computeProgress(Map(config.activityType -> config.courseActivityType, config.activityUser -> userId,
      config.contextId -> batchId, config.activityId -> courseId), leafNodes,
      courseContentsStatusFromDb.headOption.getOrElse(Map[String, AnyRef]()), csFromEvent(config.contentStatus).asInstanceOf[Map[String, AnyRef]]))
  }

  /**
   * Method to compute the progress for unit level
   */
  def getUnitProgress(csFromEvent: Map[String, AnyRef],
                      csFromDb: List[Map[String, AnyRef]],
                      context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context,
                      metrics: Metrics
                     ): Map[String, Progress] = {
    val courseId = csFromEvent.getOrElse(config.courseId, null).asInstanceOf[String]
    val batchId = csFromEvent.getOrElse(config.batchId, null).asInstanceOf[String]
    val userId = csFromEvent.getOrElse(config.userId, null).asInstanceOf[String]
    val contentStatus: Map[String, AnyRef] = csFromEvent.getOrElse(config.contentStatus, null).asInstanceOf[Map[String, AnyRef]]
    // Get the ancestors for the specific resource
    val contentAncestors: List[String] = contentStatus.map(contentId => {
      contentId._1 -> readFromCache(key = s"$courseId:${contentId._1}:${config.ancestors}", metrics).filter(_ > courseId).distinct
    }).values.flatten.toList.distinct

    // Get the leafNodes for the unit from the redis cache
    val unitLeafNodes: Map[String, List[String]] = contentAncestors.map(unitId => {
      val leafNodes: List[String] = readFromCache(key = s"$courseId:${unitId}:${config.leafNodes}", metrics).distinct
      if (leafNodes.isEmpty) throw new Exception(s"Leaf nodes are not available for this unitId:$unitId and courseId:$courseId")
      (unitId -> leafNodes.distinct)
    }).toMap

    // Get the progress for each unit node
    unitLeafNodes.map(unitLeafMap => {
      val cols = Map(config.activityType -> config.unitActivityType, config.activityUser -> userId, config.contextId -> batchId, config.activityId -> s"${unitLeafMap._1}")
      val courseContentsStatusFromDb = csFromDb.filter(cs => cs(config.courseId).asInstanceOf[String] == courseId && cs(config.batchId).asInstanceOf[String] == batchId && cs(config.userId).asInstanceOf[String] == userId)
      val progress = computeProgress(cols, unitLeafMap._2, courseContentsStatusFromDb.headOption.getOrElse(Map[String, AnyRef]()), contentStatus)
      logger.info(s"Unit: ${unitLeafMap._1} completion status: ${progress.isCompleted}")
      context.output(config.auditEventOutputTag, gson.toJson(generateTelemetry(csFromEvent, generationFor = config.unitActivityType, progress.isCompleted, unitLeafMap._1)))
      (unitLeafMap._1 -> progress)
    })
  }

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
    metrics.incCounter(config.dbReadCount)
    cassandraUtil.find(selectWhere.toString).asScala.toList

  }

  /**
   * Method to update the specific table in a batch formate
   */
  def updateDB(batchSize: Int, batch: ListBuffer[Update.Where], metrics: Metrics): Unit = {
    val cqlBatch = QueryBuilder.batch()
    batch.foreach(query => {
      if (batch.nonEmpty) {
        val totalElements = batch.slice(0, batchSize)
        totalElements.map(element => cqlBatch.add(element))
        logger.debug("cqlBatch is" + cqlBatch.toString)
        val updateStatus = cassandraUtil.upsert(cqlBatch.toString)
        if (updateStatus) {
          metrics.incCounter(config.successEventCount)
          metrics.incCounter(config.dbUpdateCount)
          batch.remove(0, totalElements.size)
        } else {
          logger.error("Database update has failed")
        }
      }
    })
  }

  def readFromCache(key: String, metrics: Metrics): List[String] = {
    metrics.incCounter(config.cacheHitCount)
    cache.sMembers(key).asScala.toList
  }

  def getUserAggQuery(progress: Progress, keySpace: String, table: String):
  Update.Where = {
    QueryBuilder.update(keySpace, table)
      .`with`(QueryBuilder.putAll(config.agg, progress.agg.asJava))
      .and(QueryBuilder.putAll(config.aggLastUpdated, progress.agg_last_updated.asJava))
      .where(QueryBuilder.eq(config.activityId, progress.activity_id))
      .and(QueryBuilder.eq(config.activityType, progress.activity_type))
      .and(QueryBuilder.eq(config.contextId, s"cb:${progress.context_id}"))
      .and(QueryBuilder.eq(config.activityUser, progress.user_id))
  }

  /**
   * Creates the cql query for content consumption table
   */
  def getContentConsumptionQuery(contentConsumption: Progress, keySpace: String, table: String): List[Update.Where] = {
    contentConsumption.contentStats.map(content => {
      QueryBuilder.update(keySpace, table)
        .`with`(QueryBuilder.set(config.viewcount, content.getOrElse(config.viewcount, 0)))
        .and(QueryBuilder.set(config.completedcount, content.getOrElse(config.completedcount, 0)))
        .where(QueryBuilder.eq(config.batchId.toLowerCase(), contentConsumption.context_id))
        .and(QueryBuilder.eq(config.courseId.toLowerCase(), contentConsumption.activity_id))
        .and(QueryBuilder.eq(config.userId.toLowerCase(), contentConsumption.user_id))
        .and(QueryBuilder.eq(config.contentId.toLowerCase(), content.get(config.contentId).orNull))
    })

  }


  /**
   * Method to compute the progress by comparing the content status
   *
   * @param cols        unit agg table columns list
   * @param leafNodes   - number of leafNodes either
   * @param csFromDB    - content status from database  Map(do_54395 -> 1, do_u5oi957438 -> 2)
   * @param csFromEvent - content status from the event ex: Map(do_54395 -> 2)
   * @return Progress - It will returns the computed progress
   */

  def computeProgress(cols: Map[String, AnyRef],
                      leafNodes: List[String],
                      csFromDB: Map[String, AnyRef],
                      csFromEvent: Map[String, AnyRef]):
  Progress = {
    val contentStatusFromDB = csFromDB.getOrElse("contentStatus", Map()).asInstanceOf[Map[String, Map[String, AnyRef]]]
    val unionKeys = csFromEvent.keySet.union(contentStatusFromDB.keySet)
    val mergedContentStatus: Map[String, Int] = unionKeys.map { key =>
      key -> (if (csFromEvent.getOrElse(key, 0).asInstanceOf[Number].intValue() >= contentStatusFromDB.getOrElse(key, Map()).getOrElse("status", 0).asInstanceOf[Number].intValue()) csFromEvent.getOrElse(key, 0).asInstanceOf[Number].intValue()
      else contentStatusFromDB.getOrElse(key, Map()).getOrElse("status", 0).asInstanceOf[Number].intValue())
    }.toMap
    val completedContent = mergedContentStatus.filter(value => value._2 == config.completedStatusCode).filter(requiredNodes => leafNodes.contains(requiredNodes._1))
    val agg = Map(config.progress -> completedContent.size) // It has only completed nodes id details
    val aggUpdatedOn = Map(config.progress -> new DateTime().getMillis) // Progress updated time
    val isCompleted: Boolean = completedContent.size == leafNodes.size

    val contentStats = mergedContentStatus.map(content => {
      val completedCount = if (content._2 == config.completedStatusCode) contentStatusFromDB.getOrElse(content._1, Map()).getOrElse(config.completedcount, 0).asInstanceOf[Int] + 1 else contentStatusFromDB.getOrElse(content._1, Map()).getOrElse(config.completedcount, 0).asInstanceOf[Int]
      val viewCount = contentStatusFromDB.getOrElse(content._1, Map()).getOrElse(config.viewcount, 0).asInstanceOf[Int] + 1
      Map(config.contentId -> content._1, config.status -> content._2, config.completedcount -> completedCount, config.viewcount -> viewCount)
    }).toList

    Progress(cols.get(config.activityType).orNull.asInstanceOf[String], cols.get(config.activityUser).orNull.asInstanceOf[String],
      cols.get(config.activityId).orNull.asInstanceOf[String], cols.get(config.contextId).orNull.asInstanceOf[String],
      agg, aggUpdatedOn, isCompleted = isCompleted, contentStats = contentStats)
  }


  /**
   * Method to get the content status object in map format ex: (do_5874308329084 -> 2, do_59485345435 -> 3)
   * It always takes the highest precedence progress values for the contents ex: (do_5874308329084 -> 2, do_5874308329084 -> 1, do_59485345435 -> 3) => (do_5874308329084 -> 2, do_59485345435 -> 3)
   *
   * Ex: Map("C1"->2, "C2" ->1)
   *
   */
  def getContentStatusFromEvent(contents: List[Map[String, AnyRef]]): Map[String, Number] = {
    val data = contents.asInstanceOf[List[util.Map[String, AnyRef]]].map(content => {
      (content.asScala.toMap.get(config.contentId).orNull.asInstanceOf[String], content.asScala.toMap.get(config.status).orNull.asInstanceOf[Number])
    }).groupBy(id => id._1.asInstanceOf[String])
    data.map(status => status._2.maxBy(_._2.intValue()))
  }

  /**
   * Method to get the content status from the database
   *
   * Ex: List(Map("courseId" -> "do_43795", batchId -> "batch1", userId->"user001", contentStatus -> Map("C1"->2, "C2" ->1)))
   *
   */
  def getContentStatusFromDB(eDataBatch: List[Map[String, AnyRef]], metrics: Metrics): List[Map[String, AnyRef]] = {
    val primaryFields = Map(config.userId.toLowerCase() -> eDataBatch.map(x => x(config.userId)).toList, config.batchId.toLowerCase -> eDataBatch.map(x => x(config.batchId)).toList, config.courseId.toLowerCase -> eDataBatch.map(x => x(config.courseId)).toList)
    val records = Option(readFromDB(primaryFields, config.dbKeyspace, config.dbContentConsumptionTable, metrics))
    records.map(record => record.groupBy(col => Map(config.batchId -> col.getObject(config.batchId.toLowerCase()).asInstanceOf[String], config.userId -> col.getObject(config.userId.toLowerCase()).asInstanceOf[String], config.courseId -> col.getObject(config.courseId.toLowerCase()).asInstanceOf[String])))
      .map(groupedRecords => groupedRecords.map(groupedRecord => Map(groupedRecord._1.asInstanceOf[Map[String, String]]
        -> groupedRecord._2.flatMap(mapRec => Map(mapRec.getObject(config.contentId.toLowerCase()).asInstanceOf[String] -> Map(config.status -> mapRec.getObject(config.status), config.viewcount -> mapRec.getObject(config.viewcount), config.completedcount -> mapRec.getObject(config.completedcount)))).toMap))).toList.flatten
      .flatMap(mapObj => mapObj.keySet.map(key => Map(config.userId -> key(config.userId), config.courseId -> key(config.courseId), config.batchId -> key(config.batchId), config.contentStatus -> mapObj.values.flatten.toMap)).toList)
  }

  /**
   * Method to generate the telemetry
   */
  def generateTelemetry(primaryFields: Map[String, AnyRef], generationFor: String, isCompleted: Boolean, activityId: String)
  : TelemetryEvent = {
    generationFor.toUpperCase() match {
      case "COURSE-UNIT" =>
        TelemetryEvent(actor = ActorObject(id = primaryFields.get("courseid").orNull.asInstanceOf[String]), edata = if (isCompleted) EventData(props = Array(new DateTime().getMillis.toString), `type` = "completed") else EventData(props = Array(activityId), `type` = "start"),
          context = EventContext(cdata = Array(Map("type" -> "CourseBatch", "id" -> primaryFields.get(config.batchId.toLowerCase()).orNull).asJava)),
          `object` = EventObject(rollup = Map("l1" -> primaryFields.get(config.courseId.toLowerCase()).orNull.asInstanceOf[String]).asJava, id = activityId, `type` = "CourseUnit")
        )
      case "CONTENTS" =>
        TelemetryEvent(
          actor = ActorObject(id = primaryFields.get(config.userId.toLowerCase()).orNull.asInstanceOf[String]),
          edata = if (isCompleted) EventData(props = Array(new DateTime().getMillis.toString), `type` = "completed") else EventData(props = Array(activityId), `type` = "start"),
          context = EventContext(cdata = Array(Map("type" -> "CourseBatch", "id" -> primaryFields.get(config.batchId.toLowerCase()).orNull).asJava)),
          `object` = EventObject(rollup = Map("l1" -> primaryFields.get(config.courseId.toLowerCase()).orNull.asInstanceOf[String], "l2" -> primaryFields.get(config.batchId.toLowerCase()).orNull.asInstanceOf[String]).asJava, id = activityId, `type` = "Content")
        )
      case "COURSE" => logger.debug("Telemetry is not generated")
        null
    }

  }

}
